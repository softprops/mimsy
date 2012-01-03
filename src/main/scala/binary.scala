package mimsy

// scala wishlist. byte literal syntax 0b00 by default is of type byte

trait Validation {
  def validKey(k: Array[Byte]) = k.size < 250
}

// http://code.google.com/p/memcached/wiki/BinaryProtocolRevamped

object Binary extends Validation {

  def b(x: Byte): Byte = x

  // http://code.google.com/p/memcached/wiki/BinaryProtocolRevamped#Magic_Byte

  object Magic {
    val Request = 0x80.toByte
    val Response = 0x81.toByte
  }

  // http://code.google.com/p/memcached/wiki/BinaryProtocolRevamped#Response_Status

  object Status {
    val Ok = b(0x0000)
    val KeyNotFound = b(0x0001)
    val KeyExists = b(0x0002)
    val ValueTooLarge = b(0x0003)
    val InvalidArgs = b(0x0004)
    val ItemNotStored = b(0x0005)
    val IncrDecrNonNum = b(0x0006)
    val InvalidVBucket = b(0x0007)
    val AuthError = b(0x0008)
    val AuthContinue = b(0x0009)
    val UnknownCmd = 0x0081.toByte
    val OutOfMem = 0x0082.toByte
    val NotSupported = 0x0083.toByte
    val InternalError = 0x0084.toByte
    val Busy = 0x0085.toByte
    val TemporaryFailure = 0x0086.toByte

    val AuthRequired = b(0x20)
    val AuthStep = b(0x21)
  }

  // http://code.google.com/p/memcached/wiki/BinaryProtocolRevamped#Command_Opcodes

  object Cmd {
    val Get = b(0x00)
    val Set = b(0x01)
    val Add = b(0x02)
    val Replace = b(0x03)
    val Delete = b(0x04)
    val Incr = b(0x05)
    val Decr = b(0x06)
    val Quit = b(0x07)
    val Flush = b(0x08)
    val GetQ = b(0x09)
    val Noop = b(0x0a)
    val Version = b(0x0b)
    val GetK = b(0x0c)
    val GetKQ = b(0x0d)
    val Append = b(0x0e)
    val Prepend = b(0x0f)
    val Stat = b(0x10)
    val SetQ = b(0x11)
    val AddQ = b(0x12)
    val RespaceQ = b(0x13)
    val DelQ = b(0x14)
    val IncrQ = b(0x15)
    val DecrQ = b(0x16)
    val QuitQ = b(0x17)
    val FlushQ = b(0x18)
    val AppendQ = b(0x19)
    val PrependQ = b(0x1a)
    val Verbosity = b(0x1b)
    val Touch = b(0x1c)
    val GAT = b(0x1d)
    val GATQ = b(0x1e)
    val SaslList = b(0x20)
    val SaslAuth = b(0x21)
    val SaslStep = b(0x22)
    val RGet = b(0x30)
    val RSet = b(0x31)
    val RsetQ = b(0x32)
    val RAppend = b(0x33)
    val RAppendQ = b(0x34)
    val RPrepend = b(0x35)
    val RPrependQ = b(0x36)
    val RDelete = b(0x37)
    val RDeleteQ = b(0x38)
    val RIncr = b(0x39)
    val RIncrQ = b(0x3a)
    val RDescr = b(0x3b)
    val RDescrQ = b(0x3c)
    val VBucket = b(0x3d)
    val GetVBucket = b(0x3e)
    val DelVBucket = b(0x3f)
    val TapConnect = b(0x40)
    val TapMutation = b(0x41)
    val TapDelete = b(0x42)
    val TapFlush = b(0x43)
    val TapOpaque = b(0x44)
    val TapVBucketSize = b(0x45)
    val TapCheckpointStart = b(0x46)
    val TapCheckoutEnd = b(0x47)
  }

  // http://code.google.com/p/memcached/wiki/BinaryProtocolRevamped#Data_Types

  object Data {
    val Raw = b(0x00)
  }
}
