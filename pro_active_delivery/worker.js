/**
 * worker.js
 * 
 * Main file for fetching and processing data (companies, contacts, and meetings) from HubSpot.
 * Includes queue management for batch insertion and ensures minimal concurrency issues.
 */

const hubspot = require('@hubspot/api-client');
const { queue } = require('async');
const _ = require('lodash');

const { filterNullValuesFromObject, goal } = require('./utils');
const Domain = require('./Domain');

/**
 * Initialize HubSpot Client instance (token set dynamically upon refresh).
 */
const hubspotClient = new hubspot.Client({ accessToken: '' });

/**
 * Track the expiration date of the current HubSpot access token.
 */
let expirationDate;

/**
 * Helper to generate a lastModifiedDate filter for HubSpot objects.
 * 
 * @param {Date} date - The last date we pulled data.
 * @param {Date} nowDate - The current date/time to compare with.
 * @param {string} propertyName - The object property used to filter by last modified date.
 * @returns {object} A filter object for HubSpot search queries.
 */
function generateLastModifiedDateFilter(date, nowDate, propertyName = 'hs_lastmodifieddate') {
  if (!date) {
    return {};
  }

  return {
    filters: [
      { propertyName, operator: 'GTQ', value: \`\${date.valueOf()}\` },
      { propertyName, operator: 'LTQ', value: \`\${nowDate.valueOf()}\` }
    ]
  };
}

/**
 * Saves domain updates to the database (stubbed out for testing).
 * 
 * @param {object} domain - The domain object whose data might have changed.
 */
async function saveDomain(domain) {
  // Currently disabled to avoid unnecessary DB writes during testing.
  // Uncomment for actual usage:
  // domain.markModified('integrations.hubspot.accounts');
  // await domain.save();
  return;
}

/**
 * Refreshes the HubSpot access token if it is expired or about to expire.
 * 
 * @param {object} domain - The Domain object containing hubspot info.
 * @param {string} hubId - The unique HubSpot portal ID for the account.
 * @param {number} [tryCount=0] - Number of times we retried refreshing the token.
 * @returns {Promise<boolean>} Promise that resolves to true if refreshed successfully.
 */
async function refreshAccessToken(domain, hubId, tryCount = 0) {
  try {
    const { HUBSPOT_CID, HUBSPOT_CS } = process.env;
    const account = domain.integrations.hubspot.accounts.find((acc) => acc.hubId === hubId);

    if (!account) {
      console.error('No account found to refresh token.');
      return false;
    }

    const { accessToken, refreshToken } = account;

    const result = await hubspotClient.oauth.tokensApi.createToken(
      'refresh_token',
      undefined,
      undefined,
      HUBSPOT_CID,
      HUBSPOT_CS,
      refreshToken
    );

    const body = result.body ? result.body : result;
    const newAccessToken = body.accessToken;

    expirationDate = new Date(body.expiresIn * 1000 + new Date().getTime());
    hubspotClient.setAccessToken(newAccessToken);

    if (newAccessToken !== accessToken) {
      account.accessToken = newAccessToken;
    }

    return true;

  } catch (error) {
    console.error('Error refreshing access token:', error);
    return false;
  }
}

/**
 * Retrieves and processes recently modified HubSpot companies in batches.
 * 
 * @param {object} domain - The domain object with integration info.
 * @param {string} hubId - The HubSpot portal ID.
 * @param {object} q - The queue instance for batched actions.
 * @returns {Promise<boolean>} Indicates success or failure of company processing.
 */
async function processCompanies(domain, hubId, q) {
  try {
    const account = domain.integrations.hubspot.accounts.find((acc) => acc.hubId === hubId);
    if (!account) {
      console.error('processCompanies: No account found for hubId', hubId);
      return false;
    }

    const lastPulledDate = new Date(account.lastPulledDates.companies);
    const now = new Date();

    let hasMore = true;
    const offsetObject = {};
    const limit = 100;

    while (hasMore) {
      const lastModifiedDate = offsetObject.lastModifiedDate || lastPulledDate;
      const lastModifiedDateFilter = generateLastModifiedDateFilter(lastModifiedDate, now);

      const searchObject = {
        filterGroups: [lastModifiedDateFilter],
        sorts: [{ propertyName: 'hs_lastmodifieddate', direction: 'ASCENDING' }],
        properties: [
          'name',
          'domain',
          'country',
          'industry',
          'description',
          'annualrevenue',
          'numberofemployees',
          'hs_lead_status'
        ],
        limit,
        after: offsetObject.after
      };

      let searchResult = null;
      let tryCount = 0;

      // Attempt to fetch data up to 4 times, backoff if needed.
      while (tryCount <= 4) {
        try {
          searchResult = await hubspotClient.crm.companies.searchApi.doSearch(searchObject);
          break;
        } catch (err) {
          tryCount++;
          console.warn('Error fetching companies, retry attempt:', tryCount, err);

          if (new Date() > expirationDate) {
            await refreshAccessToken(domain, hubId);
          }

          await new Promise((resolve) => setTimeout(resolve, 5000 * Math.pow(2, tryCount)));
        }
      }

      if (!searchResult) {
        throw new Error('Failed to fetch companies for the 4th time. Aborting.');
      }

      const data = searchResult.results || [];
      offsetObject.after = parseInt(searchResult?.paging?.next?.after);

      console.log('Fetched company batch:', data.length);

      data.forEach((company) => {
        if (!company.properties) return;

        const isCreated = !lastPulledDate || new Date(company.createdAt) > lastPulledDate;
        const actionTemplate = {
          includeInAnalytics: 0,
          companyProperties: {
            company_id: company.id,
            company_domain: company.properties.domain,
            company_industry: company.properties.industry
          }
        };

        q.push({
          actionName: isCreated ? 'Company Created' : 'Company Updated',
          actionDate: new Date(isCreated ? company.createdAt : company.updatedAt) - 2000,
          ...actionTemplate
        });
      });

      if (!offsetObject?.after) {
        hasMore = false;
      } else if (offsetObject?.after >= 9900) {
        offsetObject.after = 0;
        offsetObject.lastModifiedDate = new Date(data[data.length - 1].updatedAt).valueOf();
      }
    }

    account.lastPulledDates.companies = now;
    await saveDomain(domain);

    return true;

  } catch (error) {
    console.error('processCompanies error:', error);
    return false;
  }
}

/**
 * Retrieves and processes recently modified HubSpot contacts in batches.
 * 
 * @param {object} domain - The domain object with integration info.
 * @param {string} hubId - The HubSpot portal ID.
 * @param {object} q - The queue instance for batched actions.
 * @returns {Promise<boolean>} Indicates success or failure of contact processing.
 */
async function processContacts(domain, hubId, q) {
  try {
    const account = domain.integrations.hubspot.accounts.find((acc) => acc.hubId === hubId);
    if (!account) {
      console.error('processContacts: No account found for hubId', hubId);
      return false;
    }

    const lastPulledDate = new Date(account.lastPulledDates.contacts);
    const now = new Date();

    let hasMore = true;
    const offsetObject = {};
    const limit = 100;

    while (hasMore) {
      const lastModifiedDate = offsetObject.lastModifiedDate || lastPulledDate;
      const lastModifiedDateFilter = generateLastModifiedDateFilter(lastModifiedDate, now, 'lastmodifieddate');

      const searchObject = {
        filterGroups: [lastModifiedDateFilter],
        sorts: [{ propertyName: 'lastmodifieddate', direction: 'ASCENDING' }],
        properties: [
          'firstname',
          'lastname',
          'jobtitle',
          'email',
          'hubspotscore',
          'hs_lead_status',
          'hs_analytics_source',
          'hs_latest_source'
        ],
        limit,
        after: offsetObject.after
      };

      let searchResult = null;
      let tryCount = 0;

      while (tryCount <= 4) {
        try {
          searchResult = await hubspotClient.crm.contacts.searchApi.doSearch(searchObject);
          break;
        } catch (err) {
          tryCount++;
          console.warn('Error fetching contacts, retry attempt:', tryCount, err);

          if (new Date() > expirationDate) {
            await refreshAccessToken(domain, hubId);
          }

          await new Promise((resolve) => setTimeout(resolve, 5000 * Math.pow(2, tryCount)));
        }
      }

      if (!searchResult) {
        throw new Error('Failed to fetch contacts for the 4th time. Aborting.');
      }

      const data = searchResult.results || [];
      offsetObject.after = parseInt(searchResult?.paging?.next?.after);

      console.log('Fetched contact batch:', data.length);

      // Identify all contact IDs to fetch associations
      const contactIds = data.map((contact) => contact.id);

      // Build a read request for company associations
      const contactsToAssociate = contactIds;
      let companyAssociationsResults = [];
      try {
        const associationResp = await hubspotClient.apiRequest({
          method: 'post',
          path: '/crm/v3/associations/CONTACTS/COMPANIES/batch/read',
          body: { inputs: contactsToAssociate.map((contactId) => ({ id: contactId })) }
        });
        companyAssociationsResults = (await associationResp.json()).results || [];
      } catch (associationError) {
        console.error('Error fetching contact-company associations:', associationError);
      }

      // Create a map from contactId to associated companyId
      const companyAssociations = Object.fromEntries(
        companyAssociationsResults
          .map((association) => {
            if (association.from) {
              contactsToAssociate.splice(contactsToAssociate.indexOf(association.from.id), 1);
              return [association.from.id, association.to[0]?.id];
            }
            return false;
          })
          .filter((x) => x)
      );

      // Process each contact record and push to queue
      data.forEach((contact) => {
        if (!contact.properties || !contact.properties.email) return;

        const companyId = companyAssociations[contact.id];
        const isCreated = new Date(contact.createdAt) > lastPulledDate;

        const userProperties = {
          company_id: companyId,
          contact_name: ((contact.properties.firstname || '') + ' ' + (contact.properties.lastname || '')).trim(),
          contact_title: contact.properties.jobtitle,
          contact_source: contact.properties.hs_analytics_source,
          contact_status: contact.properties.hs_lead_status,
          contact_score: parseInt(contact.properties.hubspotscore) || 0
        };

        q.push({
          actionName: isCreated ? 'Contact Created' : 'Contact Updated',
          actionDate: new Date(isCreated ? contact.createdAt : contact.updatedAt),
          includeInAnalytics: 0,
          identity: contact.properties.email,
          userProperties: filterNullValuesFromObject(userProperties)
        });
      });

      if (!offsetObject?.after) {
        hasMore = false;
      } else if (offsetObject?.after >= 9900) {
        offsetObject.after = 0;
        offsetObject.lastModifiedDate = new Date(data[data.length - 1].updatedAt).valueOf();
      }
    }

    account.lastPulledDates.contacts = now;
    await saveDomain(domain);

    return true;

  } catch (error) {
    console.error('processContacts error:', error);
    return false;
  }
}

/**
 * Retrieves and processes recently modified HubSpot meetings in batches.
 * 
 * @param {object} domain - The domain object with integration info.
 * @param {string} hubId - The HubSpot portal ID.
 * @param {object} q - The queue instance for batched actions.
 * @returns {Promise<boolean>} Indicates success or failure of meeting processing.
 */
async function processMeetings(domain, hubId, q) {
  try {
    const account = domain.integrations.hubspot.accounts.find((a) => a.hubId === hubId);
    if (!account) {
      console.error('processMeetings: No account found for hubId', hubId);
      return false;
    }

    const lastPulledDate = new Date(account.lastPulledDates.meetings);
    const now = new Date();

    let hasMore = true;
    const offsetObject = {};
    const limit = 100;

    while (hasMore) {
      const lastModifiedDate = offsetObject.lastModifiedDate || lastPulledDate;
      const lastModifiedDateFilter = generateLastModifiedDateFilter(lastModifiedDate, now, 'hs_lastmodifieddate');

      const searchObject = {
        filterGroups: [lastModifiedDateFilter],
        sorts: [{ propertyName: 'hs_lastmodifieddate', direction: 'ASCENDING' }],
        properties: [
          'hs_meeting_title',
          'hs_meeting_start_time',
          'hs_meeting_end_time',
          'hs_createdate',
          'hs_lastmodifieddate'
        ],
        limit,
        after: offsetObject.after
      };

      let searchResult = null;
      let tryCount = 0;

      while (tryCount <= 4) {
        try {
          searchResult = await hubspotClient.crm.meetings.searchApi.doSearch(searchObject);
          break;
        } catch (err) {
          tryCount++;
          console.warn('Error fetching meetings, retry attempt:', tryCount, err);

          if (new Date() > expirationDate) {
            await refreshAccessToken(domain, hubId);
          }

          await new Promise((resolve) => setTimeout(resolve, 5000 * Math.pow(2, tryCount)));
        }
      }

      if (!searchResult) {
        throw new Error('Failed to fetch meetings for the 4th time. Aborting.');
      }

      const data = searchResult.results || [];
      offsetObject.after = parseInt(searchResult?.paging?.next?.after);

      console.log('Fetched meeting batch:', data.length);

      // Collect meeting IDs for contact associations
      const meetingIds = data.map((m) => m.id);
      let meetingContactAssociationsResponse = null;

      try {
        if (meetingIds.length > 0) {
          meetingContactAssociationsResponse = await hubspotClient.apiRequest({
            method: 'post',
            path: '/crm/v3/associations/MEETINGS/CONTACTS/batch/read',
            body: {
              inputs: meetingIds.map((id) => ({ id }))
            }
          });
        }
      } catch (err) {
        console.error('Error fetching meeting-contact associations:', err);
      }

      const meetingContactAssociations = meetingContactAssociationsResponse
        ? (await meetingContactAssociationsResponse.json()).results || []
        : [];

      // Build a map of meetingId => array of associated contactIds
      const meetingToContactsMap = {};
      meetingContactAssociations.forEach((assoc) => {
        if (!assoc.from || !assoc.to) return;
        const mid = assoc.from.id;
        const contactIds = assoc.to.map((t) => t.id);
        if (!meetingToContactsMap[mid]) {
          meetingToContactsMap[mid] = [];
        }
        meetingToContactsMap[mid].push(...contactIds);
      });

      // Gather all unique contactIds from the associations
      const allContactIds = Object.values(meetingToContactsMap).flat();
      const uniqueContactIds = [...new Set(allContactIds)];

      // Fetch emails for these contactIds
      let contactIdToEmail = {};
      if (uniqueContactIds.length > 0) {
        try {
          const contactsBatchResponse = await hubspotClient.crm.contacts.batchApi.read({
            inputs: uniqueContactIds.map((id) => ({ id })),
            properties: ['email']
          });

          if (contactsBatchResponse && contactsBatchResponse.results) {
            contactIdToEmail = Object.fromEntries(
              contactsBatchResponse.results.map((c) => [c.id, c.properties.email])
            );
          }
        } catch (err) {
          console.error('Error fetching contacts for meetings:', err);
        }
      }

      // Process each meeting record
      data.forEach((meeting) => {
        const isCreated = new Date(meeting.properties.hs_createdate) > lastPulledDate;
        const actionName = isCreated ? 'Meeting Created' : 'Meeting Updated';
        const actionDate = new Date(isCreated
          ? meeting.properties.hs_createdate
          : meeting.properties.hs_lastmodifieddate
        );

        const userProperties = {
          meeting_title: meeting.properties.hs_meeting_title,
          meeting_start_time: meeting.properties.hs_meeting_start_time,
          meeting_end_time: meeting.properties.hs_meeting_end_time
        };

        const contactIds = meetingToContactsMap[meeting.id] || [];
        contactIds.forEach((cid) => {
          const email = contactIdToEmail[cid];
          if (!email) return;

          q.push({
            actionName,
            actionDate,
            includeInAnalytics: 0,
            identity: email,
            userProperties: filterNullValuesFromObject(userProperties)
          });
        });
      });

      if (!offsetObject?.after) {
        hasMore = false;
      } else if (offsetObject?.after >= 9900) {
        offsetObject.after = 0;
        offsetObject.lastModifiedDate = new Date(
          data[data.length - 1].properties.hs_lastmodifieddate
        ).valueOf();
      }
    }

    account.lastPulledDates.meetings = now;
    await saveDomain(domain);

    return true;

  } catch (error) {
    console.error('processMeetings error:', error);
    return false;
  }
}

/**
 * Creates a queue to manage actions in memory, flushing to the goal (database) periodically.
 * 
 * @param {object} domain - The domain object for logging reference.
 * @param {Array} actions - The in-memory array of actions to push.
 * @returns {object} The queue instance.
 */
function createQueue(domain, actions) {
  return queue(async (action, callback) => {
    actions.push(action);

    // If we exceed 2000 actions, flush them to the database via 'goal'
    if (actions.length > 2000) {
      console.log('Flushing actions to database:', { apiKey: domain.apiKey, count: actions.length });

      const copyOfActions = _.cloneDeep(actions);
      actions.splice(0, actions.length);

      goal(copyOfActions);
    }
    callback();
  }, 100000000);
}

/**
 * Drains the queue and then flushes any remaining actions to the database.
 * 
 * @param {object} domain - The domain object for logging reference.
 * @param {Array} actions - The in-memory array of actions.
 * @param {object} q - The queue instance to be drained.
 * @returns {Promise<boolean>}
 */
async function drainQueue(domain, actions, q) {
  // Await the queue to complete any pending tasks
  if (q.length() > 0) {
    await q.drain();
  }

  // Flush any remaining actions
  if (actions.length > 0) {
    goal(actions);
  }

  return true;
}

/**
 * Main entry point to pull data from HubSpot for all connected accounts.
 * Processes contacts, companies, and meetings in sequence.
 */
async function pullDataFromHubspot() {
  console.log('Starting data pull from HubSpot...');

  let domain;
  try {
    domain = await Domain.findOne({});
  } catch (err) {
    console.error('Error finding domain in database:', err);
    process.exit(1);
  }

  if (!domain) {
    console.error('No domain found. Exiting...');
    process.exit(1);
  }

  if (!domain.integrations || !domain.integrations.hubspot || !domain.integrations.hubspot.accounts) {
    console.error('No HubSpot accounts found. Exiting...');
    process.exit(1);
  }

  for (const account of domain.integrations.hubspot.accounts) {
    console.log('Processing HubSpot account:', account.hubId);

    // Refresh token for each account if necessary
    try {
      const tokenRefreshed = await refreshAccessToken(domain, account.hubId);
      if (!tokenRefreshed) {
        console.warn('Token refresh failed or incomplete:', account.hubId);
      }
    } catch (err) {
      console.error('Error refreshing token for account:', account.hubId, err);
    }

    const actions = [];
    const q = createQueue(domain, actions);

    try {
      await processContacts(domain, account.hubId, q);
      console.log('Contacts processed.');
    } catch (err) {
      console.error('Error processing contacts:', err);
    }

    try {
      await processCompanies(domain, account.hubId, q);
      console.log('Companies processed.');
    } catch (err) {
      console.error('Error processing companies:', err);
    }

    try {
      await processMeetings(domain, account.hubId, q);
      console.log('Meetings processed.');
    } catch (err) {
      console.error('Error processing meetings:', err);
    }

    // Flush all queued actions for this account
    try {
      await drainQueue(domain, actions, q);
      console.log('Queue drained for account:', account.hubId);
    } catch (err) {
      console.error('Error draining queue:', err);
    }

    // Persist any domain changes
    try {
      await saveDomain(domain);
    } catch (err) {
      console.error('Error saving domain:', err);
    }

    console.log('Finished processing account:', account.hubId);
  }

  console.log('All accounts processed. Exiting...');
  process.exit();
}

module.exports = pullDataFromHubspot;
