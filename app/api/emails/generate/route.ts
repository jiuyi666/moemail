import { NextResponse } from "next/server"
import { nanoid } from "nanoid"
import { createDb } from "@/lib/db"
import { emails, messages } from "@/lib/schema"
import { eq, and, gt, sql } from "drizzle-orm"
import { EXPIRY_OPTIONS } from "@/types/email"
import { EMAIL_CONFIG } from "@/config"
import { getRequestContext } from "@cloudflare/next-on-pages"
import { getUserId } from "@/lib/apiKey"
import { getUserRole } from "@/lib/auth"
import { ROLES } from "@/lib/permissions"

export const runtime = "edge"
const EXPIRED_EMAIL_CLEANUP_BATCH_SIZE = 200
const EXPIRED_EMAIL_CLEANUP_MAX_BATCHES = 8

type RuntimeEnv = ReturnType<typeof getRequestContext>["env"]

type CleanupSummary = {
  attempted: boolean
  batches: number
  deletedEmails: number
  deletedMessages: number
}

function normalizeDomain(domain: string): string {
  return String(domain || "").trim().toLowerCase()
}

function parseDomainList(domainString: string | null): string[] {
  if (!domainString) return ["moemail.app"]
  const domains = domainString
    .split(',')
    .map(item => normalizeDomain(item))
    .filter(Boolean)
  return domains.length ? Array.from(new Set(domains)) : ["moemail.app"]
}

function isUniqueAddressError(error: unknown): boolean {
  const text = stringifyUnknownError(error).toLowerCase()
  return text.includes("unique") && text.includes("emails.address")
}

function isTransientDbError(error: unknown): boolean {
  const text = stringifyUnknownError(error).toLowerCase()
  return (
    text.includes("database is locked") ||
    text.includes("too many requests") ||
    text.includes("temporarily unavailable")
  )
}

function isDatabaseFullError(error: unknown): boolean {
  return stringifyUnknownError(error).toLowerCase().includes("exceeded maximum db size")
}

async function cleanupExpiredEmails(
  env: RuntimeEnv,
  options: {
    excludeEmailId?: string
  } = {}
): Promise<CleanupSummary> {
  const summary: CleanupSummary = {
    attempted: true,
    batches: 0,
    deletedEmails: 0,
    deletedMessages: 0,
  }

  const now = Date.now()

  for (let batch = 0; batch < EXPIRED_EMAIL_CLEANUP_MAX_BATCHES; batch += 1) {
    const bindValues: Array<string | number> = [now]
    let selectSql = `
      SELECT id
      FROM email
      WHERE expires_at < ?
    `

    if (options.excludeEmailId) {
      selectSql += " AND id != ?"
      bindValues.push(options.excludeEmailId)
    }

    selectSql += `
      ORDER BY expires_at ASC
      LIMIT ?
    `
    bindValues.push(EXPIRED_EMAIL_CLEANUP_BATCH_SIZE)

    const expiredRows = await env.DB
      .prepare(selectSql)
      .bind(...bindValues)
      .all<{ id: string }>()

    const ids = (expiredRows.results || [])
      .map(row => String(row.id || "").trim())
      .filter(Boolean)

    if (!ids.length) {
      break
    }

    summary.batches += 1

    const placeholders = ids.map(() => "?").join(", ")
    const deleteMessagesResult = await env.DB
      .prepare(`DELETE FROM message WHERE emailId IN (${placeholders})`)
      .bind(...ids)
      .run()

    summary.deletedMessages += Number(deleteMessagesResult.meta?.changes ?? 0)

    const deleteEmailsResult = await env.DB
      .prepare(`DELETE FROM email WHERE id IN (${placeholders})`)
      .bind(...ids)
      .run()

    summary.deletedEmails += Number(deleteEmailsResult.meta?.changes ?? 0)

    if (ids.length < EXPIRED_EMAIL_CLEANUP_BATCH_SIZE) {
      break
    }
  }

  return summary
}

async function withExpiredCleanupRetry<T>(
  env: RuntimeEnv,
  operation: () => Promise<T>,
  onCleanup: (summary: CleanupSummary) => void,
  options: {
    excludeEmailId?: string
  } = {}
): Promise<T> {
  try {
    return await operation()
  } catch (error) {
    if (!isDatabaseFullError(error)) {
      throw error
    }

    const summary = await cleanupExpiredEmails(env, options)
    onCleanup(summary)

    if (summary.deletedEmails === 0 && summary.deletedMessages === 0) {
      throw error
    }

    return operation()
  }
}

function stringifyUnknownError(error: unknown): string {
  if (error instanceof Error) {
    const parts: string[] = []
    const name = String(error.name || "Error").trim()
    const message = String(error.message || "").trim()
    parts.push(message ? `${name}: ${message}` : name)

    const digest = (error as { digest?: unknown }).digest
    if (digest) {
      parts.push(`digest=${String(digest)}`)
    }

    const cause = (error as { cause?: unknown }).cause
    if (cause instanceof Error) {
      const causeName = String(cause.name || "Error").trim()
      const causeMessage = String(cause.message || "").trim()
      parts.push(causeMessage ? `cause=${causeName}: ${causeMessage}` : `cause=${causeName}`)
    } else if (cause !== undefined && cause !== null) {
      try {
        parts.push(`cause=${JSON.stringify(cause)}`)
      } catch {
        parts.push(`cause=${String(cause)}`)
      }
    }

    return parts.join(" | ")
  }

  if (typeof error === "string") return error
  try {
    return JSON.stringify(error)
  } catch {
    return String(error)
  }
}

export async function POST(request: Request) {
  const db = createDb()
  const env = getRequestContext().env
  let requestMeta: { name?: string; domain?: string; expiryTime?: number } = {}
  let cleanupSummary: CleanupSummary | null = null

  try {
    const userId = await getUserId()
    if (!userId) {
      return NextResponse.json(
        { error: "未授权：缺少用户上下文" },
        { status: 401 }
      )
    }

    let userRole: string = ROLES.CIVILIAN
    try {
      userRole = await getUserRole(userId)
    } catch (roleError) {
      console.warn("Failed to resolve user role, fallback to civilian", {
        userId,
        roleError,
      })
    }

    if (userRole !== ROLES.EMPEROR) {
      const maxEmails = await env.SITE_CONFIG.get("MAX_EMAILS") || EMAIL_CONFIG.MAX_ACTIVE_EMAILS.toString()
      const activeEmailsCount = await db
        .select({ count: sql<number>`count(*)` })
        .from(emails)
        .where(
          and(
            eq(emails.userId, userId),
            gt(emails.expiresAt, new Date())
          )
        )
      
      if (Number(activeEmailsCount[0].count) >= Number(maxEmails)) {
        return NextResponse.json(
          { error: `已达到最大邮箱数量限制 (${maxEmails})` },
          { status: 403 }
        )
      }
    }

    const { name, expiryTime, domain } = await request.json<{ 
      name: string
      expiryTime: number
      domain: string
    }>()
    const requestName = String(name || "").trim()
    const requestDomain = normalizeDomain(domain)

    requestMeta = {
      name: requestName.slice(0, 64),
      domain: requestDomain.slice(0, 128),
      expiryTime: Number(expiryTime),
    }

    if (!EXPIRY_OPTIONS.some(option => option.value === expiryTime)) {
      return NextResponse.json(
        { error: "无效的过期时间" },
        { status: 400 }
      )
    }

    const domainString = await env.SITE_CONFIG.get("EMAIL_DOMAINS")
    const domains = parseDomainList(domainString)

    if (!requestDomain || !domains.includes(requestDomain)) {
      return NextResponse.json(
        {
          error: "无效的域名",
          requestDomain,
          domains,
        },
        { status: 400 }
      )
    }

    const address = `${requestName || nanoid(8)}@${requestDomain}`
    const existingEmail = await db.query.emails.findFirst({
      where: eq(sql`LOWER(${emails.address})`, address.toLowerCase())
    })

    const now = new Date()
    const expires = expiryTime === 0 
      ? new Date('9999-01-01T00:00:00.000Z')
      : new Date(now.getTime() + expiryTime)

    if (existingEmail) {
      const nowMs = Date.now()
      const existingExpiry = existingEmail.expiresAt instanceof Date
        ? existingEmail.expiresAt.getTime()
        : new Date(existingEmail.expiresAt as unknown as string).getTime()

      if (!Number.isNaN(existingExpiry) && existingExpiry > nowMs) {
        return NextResponse.json(
          { error: "该邮箱地址已被使用" },
          { status: 409 }
        )
      }

      // 过期邮箱优先原地重置，避免历史外键脏数据导致删除失败
      try {
        await withExpiredCleanupRetry(
          env,
          () => db
            .delete(messages)
            .where(eq(messages.emailId, existingEmail.id)),
          (summary) => {
            cleanupSummary = summary
          },
          { excludeEmailId: existingEmail.id }
        )
      } catch (cleanupError) {
        console.warn("cleanup messages failed, continue to reset email", cleanupError)
      }

      await withExpiredCleanupRetry(
        env,
        () => db
          .update(emails)
          .set({
            userId,
            createdAt: now,
            expiresAt: expires
          })
          .where(eq(emails.id, existingEmail.id)),
        (summary) => {
          cleanupSummary = summary
        },
        { excludeEmailId: existingEmail.id }
      )

      return NextResponse.json({
        id: existingEmail.id,
        email: address
      })
    }
    
    const emailData: typeof emails.$inferInsert = {
      address,
      createdAt: now,
      expiresAt: expires,
      userId
    }
    
    const result = await withExpiredCleanupRetry(
      env,
      () => db.insert(emails)
        .values(emailData)
        .returning({ id: emails.id, address: emails.address }),
      (summary) => {
        cleanupSummary = summary
      }
    )
    
    return NextResponse.json({ 
      id: result[0].id,
      email: result[0].address 
    })
  } catch (error) {
    const traceId = request.headers.get("cf-ray") || request.headers.get("x-request-id") || nanoid(10)
    const detail = stringifyUnknownError(error)

    if (isUniqueAddressError(error)) {
      return NextResponse.json(
        {
          error: "该邮箱地址已被使用",
          traceId,
          requestMeta,
        },
        { status: 409 }
      )
    }

    if (isTransientDbError(error)) {
      return NextResponse.json(
        {
          error: "数据库繁忙，请稍后重试",
          detail,
          traceId,
          requestMeta,
          cleanup: cleanupSummary,
        },
        { status: 503 }
      )
    }

    console.error("Failed to generate email:", {
      traceId,
      detail,
      requestMeta,
      cleanupSummary,
      error,
    })
    return NextResponse.json(
      {
        error: `创建邮箱失败: ${detail || "未知错误"}`,
        traceId,
        requestMeta,
        cleanup: cleanupSummary,
      },
      { status: 500 }
    )
  }
} 
