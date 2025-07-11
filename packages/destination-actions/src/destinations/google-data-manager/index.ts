import type { AudienceDestinationDefinition } from '@segment/actions-core'
import type { Settings } from './generated-types'

import ingest from './ingest'

import remove from './remove'

export interface RefreshTokenResponse {
  access_token: string
  scope: string
  expires_in: number
  token_type: string
}

// For an example audience destination, refer to webhook-audiences. The Readme section is under 'Audience Support'
const destination: AudienceDestinationDefinition<Settings> = {
  name: 'Google Data Manager',
  slug: 'actions-google-data-manager',
  mode: 'cloud',

  authentication: {
    scheme: 'oauth2',
    fields: {},
    testAuthentication: (_request) => {
      // Return a request that tests/validates the user's credentials.
      // If you do not have a way to validate the authentication fields safely,
      // you can remove the `testAuthentication` function, though discouraged.
    },
    refreshAccessToken: async (request, { auth }) => {
      // Return a request that refreshes the access_token if the API supports it
      const res = await request<RefreshTokenResponse>('https://www.example.com/oauth/refresh', {
        method: 'POST',
        body: new URLSearchParams({
          refresh_token: auth.refreshToken,
          client_id: auth.clientId,
          client_secret: auth.clientSecret,
          grant_type: 'refresh_token'
        })
      })

      return { accessToken: res.data.access_token }
    }
  },
  extendRequest({ auth }) {
    return {
      headers: {
        authorization: `Bearer ${auth?.accessToken}`
      }
    }
  },

  audienceFields: {},

  audienceConfig: {
    mode: {
      type: 'synced', // Indicates that the audience is synced on some schedule; update as necessary
      full_audience_sync: false // If true, we send the entire audience. If false, we just send the delta.
    },

    // Get/Create are optional and only needed if you need to create an audience before sending events/users.
    createAudience: async (_request, _createAudienceInput) => {
      // Create an audience through the destination's API
      // Segment will save this externalId for subsequent calls; the externalId is used to keep track of the audience in our database
      return { externalId: '' }
    },

    getAudience: async (_request, _getAudienceInput) => {
      // Right now, `getAudience` will mostly serve as a check to ensure the audience still exists in the destination
      return { externalId: '' }
    }
  },

  onDelete: async (_request) => {
    // Return a request that performs a GDPR delete for the provided Segment userId or anonymousId
    // provided in the payload. If your destination does not support GDPR deletion you should not
    // implement this function and should remove it completely.
  },

  actions: {
    ingest,
    remove
  }
}

export default destination
