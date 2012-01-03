package mimsy

trait Codec[T] {
  def encode(value: T): Array[Byte]
  def decode(data: Array[Byte]): T
}

object Codec {

  implicit object StringCodec extends Codec[String] {
    def encode(value: String) = value.getBytes("utf8")
    def decode(data: Array[Byte]) = new String(data, "utf8")
  }

  implicit object BytesCodec extends Codec[Array[Byte]] {
    def encode(value: Array[Byte]) = value
    def decode(data: Array[Byte]) = data
  }

  implicit object IntCodec extends Codec[Int] {
    def encode(value: Int) = (for(i <- 0 to 3) yield {
      (value >>> ((3 - i) * 8)).toByte
    }) toArray
    def decode(data: Array[Byte]) = 
      (0 /: data.view.zipWithIndex)((a,e) =>
        e match { case (b, i) =>
          a + ((b & 0xff) << (data.size - 1 - i) * 8)
        })
  }
  
  implicit object LongCodec extends Codec[Long] {
    def encode(value: Long) = (for(i <- 0 to 7) yield {
      (value >>> ((7 - i) * 8)).toByte
    }) toArray
    def decode(data: Array[Byte]) = 
      (0L /: data.view.zipWithIndex)((a,e) =>
        e match { case (b, i) =>
          a + ((b & 0xff) << (data.size - 1 - i) * 8)
        })
  }
}
