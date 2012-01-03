package mimsy

object Memcache {
  import java.nio.charset.Charset

  val DefaultHost = "127.0.0.1"
  val DefaultPort = 11211
  val ascii = Charset.forName("US-ASCII")
}

// todo: we will eventually want a way to support multiple servers
// we'll get there when we get there...
// note: if server uses sasl, we are assume PLAIN sasl mech authentication
case class Memcache(host: String = Memcache.DefaultHost,
               port: Int = Memcache.DefaultPort,
               credentials: Option[(String, String)] = None) {
  import scala.util.control.Exception.allCatch
  import java.nio.ByteBuffer
  import ByteBuffer.wrap
  import java.nio.channels.SocketChannel
  import java.net.InetSocketAddress
  import Binary._

  override def toString = "%s (%s%s:%s)" format(
    getClass().getName(), credentials match {
      case Some((login, pass)) =>"%s:%s@" format(
        login, "*" * pass.size
      )
      case _ => ""
    }, host, port)

  private lazy val saslclient = credentials match {
    case Some((login, pass)) => Some(
      Sasl.client(host).credentials(login, pass)
    )
    case _ => None
  }

  // todo (use n channels, keyed on on hash(key) -> vnode yada yada)
  private val channel =
    SocketChannel.open(new InetSocketAddress(host, port))
  //channel.configureBlocking(false)

  def close = {
    write(request(Binary.Cmd.Quit))
    allCatch.opt { channel.close }
  }

  private def write(bs: ByteBuffer*) =
    allCatch.opt { channel.write(bs.toArray) }

  // the gets 
  // must not have extras
  // must have a key
  // must not have a value
  // (http://code.google.com/p/memcached/wiki/BinaryProtocolRevamped#Get,_Get_Quietly,_Get_Key,_Get_Key_Quietly)

  private def gets(cmd: Byte, key: Array[Byte]) =
    write(request(cmd)
      .putShort(2, key.size.toShort)
      .putInt(8, key.size), wrap(key))

  /** pipeline n - 1 get[K]Q requests, followed by a normal get[K] request */
  private def multigets(cmds: Seq[(Byte, Array[Byte])]) = {
    val reqs = (Array.empty[ByteBuffer] /: cmds.view.zipWithIndex)((a, e) => e match { 
      case ((cmd, key), i) =>
        Array(request(cmd)
              .putShort(2, key.size.toShort) // key len
              .putInt(8, key.size)  // vbucket
              .putInt(12, i), // `clever` use of opaque slot for req index
              wrap(key)) ++ a
    })
    write(reqs:_*)
  }

  def get[T](key: Array[Byte])(implicit codec: Codec[T]) = {
    gets(Cmd.Get, key)
    read(Cmd.Get, optionHandler(codec))
  }

  def getk[T](key: Array[Byte])(implicit codec: Codec[T]) = {
    gets(Cmd.GetK, key)
    read(Cmd.GetK, keyedOptionHandler(codec))
  }

  def multiget[T](firstkey: Array[Byte], others: Array[Byte]*)(
    implicit codec: Codec[T]) =
    (firstkey :: others.toList) match {
      case ks if(ks.size > 1) =>
        multigets(ks.init.map((Cmd.GetQ, _)) :+ (Cmd.Get, ks.last))
        read(Cmd.GetQ, optionsHandler(Cmd.GetQ, ks.size, Seq.empty[Option[T]], codec))
      case ks =>
        gets(Cmd.Get, ks.first)
        read(Cmd.Get, optionsHandler(Cmd.Get, ks.size, Seq.empty[Option[T]], codec))
    }

  def multigetk[T](firstkey: Array[Byte], others: Array[Byte]*)(
    implicit codec: Codec[T]) =
    (firstkey :: others.toList) match {
      case ks if(ks.size > 1) =>
        multigets(ks.init.map((Cmd.GetKQ, _)) :+ (Cmd.GetK, ks.last))
        read(Cmd.GetKQ, keyedOptionsHandler(Cmd.GetKQ, ks.size, Seq.empty[Option[(Array[Byte],T)]], codec))
      case ks =>
        gets(Cmd.GetK, ks.first)
        read(Cmd.GetK, keyedOptionsHandler(Cmd.GetK, ks.size, Seq.empty[Option[(Array[Byte],T)]], codec))
    }

  // sets
  // must have extras
  // must have key
  // must have value
  // (http://code.google.com/p/memcached/wiki/BinaryProtocolRevamped#Set,_Add,_Replace)

  private def sets(
    cmd: Byte, key: Array[Byte], value: Array[Byte],
    flags: Int, ttl: Int, casId: Long) =
      write(request(cmd, 32)
            .putShort(2, key.size.toShort)
            .put(4, 8.toByte)
            .putInt(8, key.size + value.size + 8)
            .putLong(16, casId)
            .putInt(24, flags)
            .putInt(28, ttl),
            wrap(key),
            wrap(value))

  def set[T](key: Array[Byte], value: T, flags: Int = 0, ttl: Option[Int] = None,
             casId: Option[Long] = None)(implicit codec: Codec[T]) = {
    sets(Cmd.Set, key, codec.encode(value), flags,
         ttl.getOrElse(0), casId.getOrElse(0))
    read(Cmd.Set, setsHandler(casId.getOrElse(0)))
  }
  
  def add[T](key: Array[Byte], value: T,
          ttl: Option[Int] = None,
          flags: Int = 0, casId: Option[Long] = None)(implicit codec: Codec[T]) = {
    sets(Cmd.Add, key, codec.encode(value), flags,
         ttl.getOrElse(0), casId.getOrElse(0))
    read(Cmd.Add, setsHandler(casId.getOrElse(0)))
  }

  def replace[T](
    key: Array[Byte], value: T,
    ttl: Option[Int] = None,
    casId: Option[Long] = None,
    flags: Int = 0)(implicit codec: Codec[T]) = {
    sets(Cmd.Replace, key, codec.encode(value), flags,
         ttl.getOrElse(0), casId.getOrElse(0))
    read(Cmd.Replace, setsHandler(casId.getOrElse(0)))
  }

  // deletes
  // must not have extras
  // must have key
  // must not have value
  // (http://code.google.com/p/memcached/wiki/BinaryProtocolRevamped#Delete)

  def delete(key: Array[Byte], casId: Option[Long] = None) = {
    write(request(Binary.Cmd.Delete)
          .putShort(2, key.size.toShort)
          .putInt(8, key.size)
          .putLong(16, casId.getOrElse(0)),
          wrap(key))
    read(Cmd.Delete, setsHandler(casId.getOrElse(0)))
  }

  // inc/decs
  // must have extras
  // must have key
  // must not have value
  // (http://code.google.com/p/memcached/wiki/BinaryProtocolRevamped#Increment,_Decrement)

  private def incdecs(
    cmd: Byte, key: Array[Byte], count: Long,
    ttl: Option[Int] = None, default: Option[BigInt] = None) = {
    val expires = default match {
      case None => 0xFFFFFFFF
      case Some(n) => ttl.getOrElse(0)
    }

    val buf = request(cmd, 44)
        .putShort(2, key.size.toShort)
        .put(4, 20.toByte)
        .putInt(8, 20 + key.size)
        .putLong(24, count)
        .putInt(40, expires)

    default match {
      case None => ()
      case Some(n) => {
        (0 to 7).foreach { i =>
          buf.put(i + 32, (n >> (7 - i) * 8).toByte)
        }
      }
    }
    write(buf, wrap(key))
  }

  def incr(key: Array[Byte], count: Long = 1,
           ttl: Option[Int] = None,
           default: Option[BigInt] = None) = {
    incdecs(Cmd.Incr, key, count, ttl, default)
    read(Cmd.Incr, countingHandler)
  }

 def decr(key: Array[Byte], count: Long = 1,
        ttl: Option[Int] = None,
        default: Option[BigInt] = None) = {
   incdecs(Cmd.Decr, key, count, ttl, default)
   read(Cmd.Decr, countingHandler)
 }

  // quits

  def quit = {
    write(request(Cmd.Quit))
    read(Cmd.Quit, emptyHandler)
  }

  // flushes
  
  def flush(in: Option[Int] = None) = {
    write(in match {
      case Some(secs) =>
        request(Cmd.Flush, 28)
          .put(4, 4.toByte)
          .putInt(8, 8)
          .putInt(24, secs)
      case _ =>
        request(Cmd.Flush)
    })
    read(Cmd.Flush, emptyHandler)
  }

  // noops

  def noop = {
    write(request(Cmd.Noop))
    read(Cmd.Noop, emptyHandler)
  }

  // versions

  def version = {
    write(request(Cmd.Version))
    read(Cmd.Version, stringifyHandler)
  }

  // append,prepends
  
  private def apprepends(cmd: Byte, key: Array[Byte], value: Array[Byte]) = {
    write(request(cmd)
          .putShort(2, key.size.toShort)
          .putInt(8, key.size + value.size),
          wrap(key),
          wrap(value))
  }

  def append[T](key: Array[Byte], value: T)(implicit codec: Codec[T]) = {
    val encoded = codec.encode(value)
    apprepends(Cmd.Append, key, codec.encode(value))
    read(Cmd.Append, emptyHandler)
  }

  def prepend[T](key: Array[Byte], value: T)(implicit codec: Codec[T]) = {
    apprepends(Cmd.Prepend, key, codec.encode(value))
    read(Cmd.Prepend, emptyHandler)
  }

  // stats

  def stat(key: Option[Array[Byte]] = None) = {
    key match {
      case Some(k) => write(request(Cmd.Stat).putShort(2, k.size.toShort))
      case _ => write(request(Cmd.Stat))
    }
    read(Cmd.Stat, mapHandler(Cmd.Stat, Map.empty[String, String]))
  }

  // verbosity

  // touch, gat, gatq

  // todos (a.k.a the other cmds not clearly defined)


  // sasls
  // http://code.google.com/p/memcached/wiki/SASLAuthProtocol
  // http://docs.oracle.com/javase/1.5.0/docs/guide/security/sasl/sasl-refguide.html


  /** @return seq of sasl mechanisms supported by the server */
  def sasls: Seq[String] = {
    write(request(Cmd.SaslList))
    read(Cmd.SaslList, stringifyHandler) match {
      case uk if(uk.equals("Unknown command")) => Seq.empty[String]
      case ss => ss.split("""\s+""")
    }
  }

  /** Initiates initial auth request using mech (obtainable from invoking sasls)
   *  and challenge associated with mech */
  def saslauth(mech: String = "PLAIN", challenge: Option[String] = None) = {
    val value = saslclient match {
      case Some(sc) =>
        if(sc.hasInitialResponse) sc.evaluateChallenge(Array.empty[Byte]) else Array.empty[Byte]
      case _ => sys.error(
        "missing configured sasl client"
      )
    }
    val mbytes = mech.getBytes()
    write(request(Cmd.SaslAuth)
            .putShort(2, mbytes.size.toShort)
            .putInt(8, mbytes.size + value.size),
          ByteBuffer.wrap(mbytes),
          ByteBuffer.wrap(value))
    read(Cmd.SaslAuth, saslAuthHandler(mech, challenge))
  }

  protected def saslstep(mech: String, challenge: Option[String]) = {
    write(request(Cmd.SaslStep))
  }

  // response handlers

  private def saslAuthHandler(mech: String, challenge: Option[String])(
    head: ByteBuffer, body: ByteBuffer) = {
    head.getShort(6) match {
      case Status.AuthRequired =>
        println("auth required or unsuccessful")
      case Status.AuthStep =>
        println("exec next auth step")
      case stat => sys.error(
        "unexpected sasl auth status %s" format stat)
    }
  }

  private def mapHandler(cmd: Byte, values: Map[String, String])(
    head: ByteBuffer, body: ByteBuffer): Map[String, String] = {
    head.getShort(6) match {
      case Status.Ok =>
        head.getShort(2).toInt match {
          case klen if(klen > 0) =>
            val bary = body.array
            val xlen = head.get(4).toInt
            val key = bary.slice(xlen, xlen + klen)
            val data = bary.slice(xlen + klen, body.capacity)
            read(cmd, mapHandler(cmd, values + (
              new String(key) -> new String(data))))
          case _ => values
        }
      case stat => sys.error(
        "Unexpected status %s" format stat
      )
    }
  }

  private def emptyHandler(head: ByteBuffer, body: ByteBuffer) = 
    head.getShort(6) match {
      case Status.Ok  => ()
      case stat => sys.error("Unexpected status %d" format stat)
    }

  private def countingHandler(head: ByteBuffer, body: ByteBuffer) =
    head.getShort(6) match {
      case Status.Ok => Some(BigInt(1, body.array))
      case Status.KeyNotFound => None
      case Status.IncrDecrNonNum => sys.error("bad inc/decr")
      case stat => sys.error("Unexpected status %d" format stat)
    }

  private def setsHandler(casId: Long)(head: ByteBuffer, body: ByteBuffer) =
    head.getShort(6) match {
      case Status.Ok =>
        head.get(16).toLong match {
          case cas if(cas > 0) =>
            if(cas == casId) true
            else sys.error(
              "cas mismatch: %d is not %d" format(cas, casId)
            )
          case _ => true
        }
      case Status.KeyNotFound => false
      case Status.KeyExists => false
      case Status.ItemNotStored => false
      case Status.ValueTooLarge => sys.error(
        "value too large"
      )
      case stat =>
        sys.error("Unexpected status %d" format(stat))
    }
  
  private def keyedOptionHandler[T](codec: Codec[T])(
    head: ByteBuffer, body: ByteBuffer): Option[(Array[Byte], T)] =
      head.getShort(6) match {
        case Status.Ok =>
          val bary = body.array
          val klen = head.getShort(2).toInt
          val xlen = head.get(4).toInt
          val key = bary.slice(xlen, xlen + klen)
          val data = bary.slice(xlen + klen, body.capacity)
          Some(key, codec.decode(data))
        case s => None
      }

  // todo: a better ret type may be a map Array[Byte]->T
  private def keyedOptionsHandler[T](cmd:Byte, n: Int, res: Seq[Option[(Array[Byte],T)]], codec: Codec[T])(
    head: ByteBuffer, body: ByteBuffer): Seq[Option[(Array[Byte], T)]] =
      (head.getShort(6), head.getInt(12)) match {
        case (Status.Ok, opaque) =>
          val bary = body.array
          val klen = head.getShort(2).toInt
          val xlen = head.get(4).toInt
          val key = bary.slice(xlen, xlen + klen)
          val data = bary.slice(xlen + klen, body.capacity)
          val value = Some(key, codec.decode(data))

          opaque match {
            case i if (i == res.size) => // append
               val appended = res :+ value
               if(i < n - 1) read(cmd, keyedOptionsHandler(cmd, n, appended, codec)) // more plz
               else appended 
            case i if (i > res.size) => // needs fill
              val diff = i - res.size
              val filled = res ++ Seq.fill[Option[(Array[Byte],T)]](diff)(None) :+ value
              if(i < n - 1) read(cmd, keyedOptionsHandler(cmd, n, filled, codec)) // more plz
              else filled
          }
        case (s, opaque) =>
          opaque match {
            case i if (i == res.size) => // append
               val appended = res :+ None
               if(i < n - 1) read(cmd, keyedOptionsHandler(cmd, n, appended, codec)) // more plz
               else appended 
            case i if (i > res.size) => // needs fill
              val diff = i - res.size
              val filled = res ++ Seq.fill[Option[(Array[Byte],T)]](diff)(None) :+ None
              if(i < n - 1) read(cmd, keyedOptionsHandler(cmd, n, filled, codec)) // more plz
              else filled
          }
      }

  private def optionHandler[T](codec: Codec[T])(
    head: ByteBuffer, body: ByteBuffer): Option[T] =
      head.getShort(6) match {
        case Status.Ok =>
          val xlen = head.get(4).toInt
          val data = body.array.slice(xlen, body.capacity)
          Some(codec.decode(data))
        case Status.AuthRequired => sys.error("auth required")
        case Status.AuthStep => sys.error("next auth step")
        case s => None
      }

  private def optionsHandler[T](cmd:Byte, n: Int, res: Seq[Option[T]], codec: Codec[T])(
    head: ByteBuffer, body: ByteBuffer): Seq[Option[T]] =
      (head.getShort(6), head.getInt(12)) match {
        case (Status.Ok, opaque) =>
          val xlen = head.get(4).toInt
          val data = body.array.slice(xlen, body.capacity)
          val value = Some(codec.decode(data))
          opaque match {
            case i if (i == res.size) => // append
               val appended: Seq[Option[T]] = res :+ value
               if(i < n - 1) read(cmd, optionsHandler(cmd, n, appended, codec)) // more plz
               else appended 
            case i if (i > res.size) => // needs fill
              val diff = i - res.size
              val filled: Seq[Option[T]] = res ++ Seq.fill[Option[T]](diff)(None) :+ value
              if(i < n - 1) read(cmd, optionsHandler(cmd, n, filled, codec)) // more plz
              else filled
          }
        case (s, opaque) =>
          opaque match {
            case i if (i == res.size) => // append
               val appended: Seq[Option[T]] = res :+ None
               if(i < n - 1) read(cmd, optionsHandler(cmd, n, appended, codec)) // more plz
               else appended 
            case i if (i > res.size) => // needs fill
              val diff = i - res.size
              val filled: Seq[Option[T]] = res ++ Seq.fill[Option[T]](diff)(None) :+ None
              if(i < n - 1) read(cmd, optionsHandler(cmd, n, filled, codec)) // more plz
              else filled
          }
      }

  private def stringifyHandler(head: ByteBuffer, body: ByteBuffer) =
    head.getShort(6) match {
      case Status.Ok => new String(body.array)
      case other => sys.error("expected status 0x%x but instead got 0x%x" format(
        Status.Ok, other
      ))
    }

  /** read response from server */
  private def read[T](cmd: Byte, f: (ByteBuffer, ByteBuffer) => T): T = {
    @annotation.tailrec
    def consume(bytes: ByteBuffer, len: Int): ByteBuffer = {
      channel.read(bytes) match {
        case n if(n < len) =>
          consume(bytes, len)
        case n =>
          bytes.flip
          bytes
      }
    }

    // 24 byte allotment for header packet frame
    // http://code.google.com/p/memcached/wiki/BinaryProtocolRevamped#Response_header

    val header = consume(ByteBuffer.allocate(24), 24)

    lazy val body = {
      val len = header.getInt(8)
      consume(ByteBuffer.allocate(len), len)
    }

    if(header.get(0) != Magic.Response) sys.error(
      "expected response magic 0x%x but instead found 0x%x" format(
        Magic.Response, header.get(0))
    )
    
    //if(header.get(1) != cmd) sys.error(
    //  "expected response for cmd 0x%x (%s) but instead got response for 0x%x (%s)" format(
    //    cmd, cmd, header.get(1), header.get(1))
    //)
    
    f(header, body)
  }

  /** build a request header */
  private def request(cmd: Byte, len: Int = 24) =
    ByteBuffer.allocate(len)
      .put(0, Magic.Request)
      .put(1, cmd)
}
