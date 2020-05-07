
package modules

import com.google.inject.AbstractModule
import com.google.inject.assistedinject.FactoryModuleBuilder
import services.datamart.CSVUploadServiceFactory

/**
  * Module that starts along with the scheduler application.
  */
class GuiceConfiguration extends AbstractModule {

  override def configure(): Unit = {

    bind(classOf[GlobalContext]).asEagerSingleton()

    bind(classOf[ConsumerStarter]).asEagerSingleton()

    bind(classOf[JobStarter]).asEagerSingleton()

    install(new FactoryModuleBuilder().build(classOf[CSVUploadServiceFactory]))
  }
}
