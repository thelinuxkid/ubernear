Collection jobs
---------------
- Some events only return an id for the venue. Specify dependencies
  between the event and its venue in the batch request so that the
  venue information is always received.
- facebook-owner retrieves all events for each owner every time
  (without paging). Change this so that events are retrieved
  incrementally.
- Make using the USPS Address Information API and the Yahoo
  PlaceFinder API optional.
- Make matching event venues with the places collection optional.

API
---
- Add support for geo-spatial queries using a box region.
- Add support for filtering out on-going events by specifying the
  total time they last, e.g., more than one month.
- Return the event category if it exists.
- Add more specific time filters.
- Add text search capability.
