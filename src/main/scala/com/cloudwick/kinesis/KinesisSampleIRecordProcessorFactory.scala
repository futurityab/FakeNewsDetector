package com.cloudwick.kinesis

import com.amazonaws.services.kinesis.clientlibrary.interfaces.{IRecordProcessor, IRecordProcessorFactory}

class KinesisSampleIRecordProcessorFactory  extends IRecordProcessorFactory {

  override def createProcessor(): IRecordProcessor = {
    new KinesisSampleIRecordProcessor()
  }

  
}