package mimsy

// todo: is this worth extracting into its own lib?
// http://www.ietf.org/rfc/rfc2222.txt
// http://docs.oracle.com/javase/1.5.0/docs/guide/security/sasl/sasl-refguide.html


object Sasl {
  import javax.security.auth.callback._
  import javax.security.sasl.{ Sasl => JSasl, _ }
  import scala.collection.JavaConversions._

  type Handler = PartialFunction[Callback, Unit]

  // for more info on java's callback handlers see
  // http://docs.oracle.com/javase/1.4.2/docs/api/javax/security/auth/callback/CallbackHandler.html

  // todo: prefab handler for other callbacks like 
  // http://docs.oracle.com/javase/6/docs/api/index.html?javax/security/auth/callback/ChoiceCallback.html

  object Handlers {

    def credentials(login: String, password: String): Handler = {
      case ncb: NameCallback =>
        ncb.setName(login)
      case pcb: PasswordCallback =>
        pcb.setPassword(password.toArray)
    }

    def callbacks(handler: Handler) =
      new CallbackHandler {
        def handle(cbs: Array[Callback]) = {
          cbs.map { cb =>
            (handler.orElse {
              case _ => sys.error(
                "unhandled callback %s" format cb
              )
            }: Handler)(cb)
          }
        }
      }
  }

  case class Callbacks[T](after: CallbackHandler => T) {
    def credentials(login: String, password: String): T =
      after(Handlers.callbacks(Handlers.credentials(login, password)))
    def handler(hand: => Handler): T =
      after(Handlers.callbacks(hand))
  }
  
  /**
   * Creates a client for the sasl protocol
   *
   * To create credentials for a given authId own your host system
   * 
   *     saslpasswd2 -a {authId} -c {login}
   * 
   * usage for plain sasl mech
   *
   *     Sasl.client(host).credentials(user, pass)
   * 
   * usage for custom mech
   * 
   *    Sasl.client(host, mech).handler({
   *      case cb: SomeCallbackType => // handle it
   *    })
   */
  def client(
    host: String, authId: String = "memcached",
    mech: String = "PLAIN",
    props: Map[String, Any] = Map.empty[String, Any],
    protocol: String = null) =
      new Callbacks({
         JSasl.createSaslClient(
           Array(mech), protocol, authId,
           host, props, _)
      })

}
