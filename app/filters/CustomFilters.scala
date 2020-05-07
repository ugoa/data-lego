
package filters

import javax.inject.Inject
import play.api.http.DefaultHttpFilters

/**
  * Module that enables Filter for each requests.
  * @param log Injected instance of [[RequestLoggingFilter]]
  */
class CustomFilters @Inject()(log: RequestLoggingFilter) extends DefaultHttpFilters(log)
