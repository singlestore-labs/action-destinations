import { RequestClient, IntegrationError } from '@segment/actions-core'
import { Payload } from './generated-types'
import { Settings } from '../generated-types'
import { ExecJSONRequest, ExecJSONResponse } from '../types'
import btoa from 'btoa-lite'

export async function send(request: RequestClient, payloads: Payload[], settings: Settings): Promise<ExecJSONResponse> {
  const { host, port, username, password, dbName, tableName } = settings
  const url = `https://${host}:${port}/api/v2/exec`
  const encodedCredentials = btoa(`${username}:${password}`)

  const columns = [
    'messageId',
    'timestamp',
    'type',
    'event',
    'name',
    'properties',
    'userId',
    'anonymousId',
    'groupId',
    'traits',
    'context'
  ]

  const sqlValuesClause = payloads.map(() => `(${columns.map(() => '?').join(', ')})`).join(', ')

  const sql = `INSERT INTO \`${tableName}\` (${columns.join(', ')}) VALUES ${sqlValuesClause}`

  function toUTCDateTime(timestamp: string): string {
    const date = new Date(timestamp)
    return date.toISOString().replace('T', ' ').replace('Z', '') // 'YYYY-MM-DD HH:MM:SS'
  }

  const args: any[] = []
  for (const item of payloads) {
    args.push(
      item.messageid,
      toUTCDateTime(item.timestamp),
      item.type,
      item.event ?? null,
      item.name ?? null,
      item.properties ?? null,
      item.userId ?? null,
      item.anonymousId ?? null,
      item.groupId ?? null,
      item.traits ?? null,
      item.context ?? null
    )
  }

  const requestData: ExecJSONRequest = {
    sql,
    database: dbName,
    args
  }

  const response = await request<ExecJSONResponse>(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Basic ${encodedCredentials}`
    },
    json: requestData,
    throwHttpErrors: false
  })

  const responeData: ExecJSONResponse = response.data
  if (typeof responeData.ok === 'boolean' && responeData.ok === false) {
    throw new IntegrationError(`Failed to insert data: ${responeData.error || 'Unknown error'}`, 'Bad Request', 400)
  }
  return responeData
}
