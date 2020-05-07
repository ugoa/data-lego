
package modules

import com.google.inject.AbstractModule

/**
  * Used in `sbt console` for develoment/debugging/testing.
  */
class DevConfiguration extends AbstractModule {

  override def configure(): Unit = {
    bind(classOf[GlobalContext]).asEagerSingleton()
  }
}
