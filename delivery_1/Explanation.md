In this update, we introduced a new feature to fetch meetings from HubSpot similar to how contacts and companies are processed. 

In `Domain.js`, we added a `meetings` field to the `lastPulledDates` object so we can track the last time meetings were pulled. 

In `worker.js`, we created a new function `processMeetings` that queries HubSpot for recently modified meeting records. 

It identifies whether a meeting has been newly created or updated, and it pushes the corresponding actions (`Meeting Created` or `Meeting Updated`) into a queue. 

We also retrieve the contacts attending each meeting by calling HubSpot’s association API, then lookup the contact emails via the batch contacts endpoint. 

For each meeting, we store relevant meeting properties (title, start time, and end time) along with the email of each attending contact. 

This way, each meeting becomes a separate action record, properly associated with a contact by email. 

Finally, we integrated this new `processMeetings` function into the main workflow so that it is called along with processing contacts and companies.