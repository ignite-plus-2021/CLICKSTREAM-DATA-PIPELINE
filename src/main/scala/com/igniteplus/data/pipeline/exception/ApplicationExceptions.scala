package com.igniteplus.data.pipeline.exception


  class ApplicationExceptions(message: String, cause: Throwable) extends Exception(message,cause){
    def this(message: String) = this(message, None.orNull)
  }

  case class FileReadException (message: String) extends ApplicationExceptions(message)

  case class FileWriteException (message: String) extends ApplicationExceptions(message)

  case class DqDuplicateCheckException(message: String) extends ApplicationExceptions(message)

  case class DqNullCheckException(message: String) extends ApplicationExceptions(message)
