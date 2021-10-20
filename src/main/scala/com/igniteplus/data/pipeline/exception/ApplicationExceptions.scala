package com.igniteplus.data.pipeline.exception


  class ApplicationExceptions(message: String) extends Exception(message){
  }

  case class FileReadException (message: String) extends ApplicationExceptions(message)

  case class FileWriteException (message: String) extends ApplicationExceptions(message)

  case class DqDuplicateCheckException(message: String) extends ApplicationExceptions(message)

  case class DqNullCheckException(message: String) extends ApplicationExceptions(message)
