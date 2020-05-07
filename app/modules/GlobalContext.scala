
package modules

import javax.inject.{Inject, Singleton}

import play.api.inject.Injector

// Read https://stackoverflow.com/a/44229843/1077486
@Singleton
class GlobalContext @Inject()(playInjector: Injector) {
  GlobalContext.injectorRef = playInjector
}

object GlobalContext {
  private var injectorRef: Injector = _

  def injector: Injector = injectorRef

  // Load application and initialize injector. It's particularly helpful in sbt development console.
  def initCustomInjector(): Unit = {
    import play.api.inject.guice.GuiceApplicationBuilder
    injectorRef = new GuiceApplicationBuilder().injector
  }
}
