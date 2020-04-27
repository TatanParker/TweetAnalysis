# coding=utf-8

import util
import tweepy
import argparse
import json
import httplib

__author__ = "Enrique Rodriguez Moron"
__doc__ = """
Este fichero de Python permite escuchar sobre un/unos determinado/s temas en Twitter. Despues son procesados para 
guardar algunos de los campos y, posteriormente, se guardan en Mongodb.
=====================================================================
Parametros:
    * -ck/--consumerkey: Consumer key que se obtiene de Twitter. **Obligatorio**
    * -cs/--consumersecret: Consumer secret que se obtiene de Twitter. **Obligatorio**
    * -t/--token: Token que se obtiene de Twitter. **Obligatorio**
    * -s/--secret: Secret que se obtiene de Twitter. **Obligatorio**
    * -mdbh/--mongodbhost: Mongodb host. **Opcional**, por defecto localhost
    * -mdbp/--mongodbpuerto: Mongodb puerto. **Opcional**, por defecto 27017
    * -mdbu/--mongodbuser: Mongodb usuario. **Opcional**
    * -mdbc/--mongodbcontrasenya: Mongodb contrasenya. **Opcional**
    * -gte/--guardartweetsenteros: Guarda todos los tweets sin ser procesados en otra coleccion. 
        **Opcional**, este parametro no tiene que tener valor
    * -bat/--borraranteriorestweets: Borra los anteriores tweest almacenados. 
        **Opcional**, este parametro no tiene que tener valor
    * -lt/--limitetweets: Limite de tweets que se escuchan y almacenan. **Opcional**, -1 por defecto. 
        Cualquier valor menor a 0 se considera escucha infinita
    * -tt/--temastweets: Lista de temas en los que se esta interesado. El programa buscara tweets que contengan todos
        los parametros (and). **Opcional**, madrid por defecto

Mongodb:
    Puesto que los tweets son almacenados en Mongodb, es requisito que una instancia este arrancada y se notifique
    la direccion, puerto y usuario/contrasenya (si es necesario) al programa cuando se arranque.
    
    Se ha decidido almacenar los tweests en Mongodb por dos motivos:
        1. Los documentos son almacenados en formato JSON, el mismo formato que utiliza Twitter
        2. Son almacenados en unicode por lo que se evita los problemas de codificacion

Procesado de tweest:
    Una vez que se obtiene un nuevo tweet, se mira si es retweet para evitar que se guarden tweets que contienen el
    mismo texto y que no aporta informacion. Si es retweet, directamente se descarta y el contador de tweets no 
    aumenta por lo que no afecta al limite (ver parametros). 
    
    Una vez que se obtiene un tweet no retweet, se procesa eliminando muchos de los campos que contiene en los que no
    estamos interesados y que tampoco lo estaremos en un futuro. Se da la posibilidad tambien de guardar el tweet
    sin este procesado en una coleccion en Mongodb. Para ver que campos estan siendo guardados consultar 
    DICT_KEYS_TWEERS.
    
    Posteriormente se guarda el tweet procesado (y el no procesado si asi se ha dicho) para ser analizado.

Testeado y versiones de librerias:
    * python 2.7.14
    * tweepy 3.5.0
    * pymongo 3.6.0
    * pandas 0.21.0
    * python-dateutil 2.6.1
"""

consumer_key = "reemplazar_por_el_consumer_key_de_twitter"
consumer_secret = "reemplazar_por_el_consumer_secret_de_twitter"

token = "reemplazar_por_el_token_de_twitter"
secret = "reemplazar_por_el_secret_de_twitter"

mongodbHost = "localhost"
mongodbPuerto = 27017

# Diccionario con los campos de los tweets en los que podemos estar interesados. Los valores de las keys pueden ser:
#     * String: si se desean todos los campos de la key. El valor del string puede ser vacio o cualquier valor
#       que se quiera, se traran de la misma manera
#     * Diccionario: si se desea filtrar los campos del nivel superior
DICT_KEYS_TWEERS = {"truncated": "all",
                    "text": "all",
                    "id": "all",
                    "id_str": "all",
                    "favorite_count": "all",
                    "source": "all",
                    "retweeted": "all",
                    "quoted_status_id": "all",  # Salva el id si es un retweet
                    "quoted_status_id_str": "all",
                    "entities": {"user_mentions":
                                     {"id": "all", "id_str": "all", "screen_name": "all", "name": "all"},
                                 "hashtags":
                                     {"text": "all"},
                                 "urls":
                                     {"expanded_url": "all"}},
                    "retweet_count": "all",
                    "retweeted_status": {"truncated": "all",
                                         "text": "all",
                                         "id_str": "all",
                                         "user": {"id": "all",
                                                  "id_str": "all",
                                                  "location": "all",
                                                  "name": "all"},
                                         "place": {"id": "all",
                                                   "url": "all",
                                                   "place_type": "all",
                                                   "name": "all",
                                                   "full_name": "all",
                                                   "country_code": "all",
                                                   "country": "all",
                                                   "bounding_box": "all"}},

                    "favorited": "all",
                    "user": {"follow_request_sent": "all",
                             "id": "all",
                             "id_str": "all",
                             "verified": "all",
                             "followers_count": "all",
                             "utc_offset": "all",
                             "description": "all",
                             "friends_count": "all",
                             "following": "all",
                             "lang": "all",
                             "name": "all",
                             "created_at": "all",
                             "time_zone": "all",
                             "geo_enabled": "all",
                             "location": "all",
                             "favourites_count": "all"},
                    "geo": "all",
                    "coordinates": "all",
                    "place": {"id": "all",
                              "url": "all",
                              "place_type": "all",
                              "name": "all",
                              "full_name": "all",
                              "country_code": "all",
                              "country": "all",
                              "bounding_box": "all"},
                    "lang": "all",
                    "created_at": "all"}


class FiltroTwiter:
    """
    Clase para filtrar los campos de un tweet elminando aquellos que no estan en el diccionario y manteniendo
    los que estan (ver variable DICT_KEYS_TWEERS).

    Por ejemplo, dado el tweet
        {"text":"Hola, este es un tweet",
            "user":{"name":"ERM", "geo":"", "friends":[{"user":"Pepito"}, {"user":"Pepita"}]},
            "count":0
        }
    y el filtro
        {"text":"all",
            "user":{"name":"all", "friends": "all"}
        }
    el resultado seria
        {"text":"Hola, este es un tweet",
            "user":{"name":"ERM", "friends":[{"user":"Pepito"}, {"user":"Pepita"}]}
        }

    Esta clase se utilizara para guardar algunos campos del tweet que pueden ser interesantes ahora o en un
    posterior analisis.
    """

    def __init__(self, diccionarioParaFiltrar={}):
        """
        Crea el objeto con el diccionario para filtrar que se desea

        :param diccionarioParaFiltrar: Diccionario que se utilizara para filtrar tweets en formato JSON. Este
            diccionario tendra como key los campos que se quieren incluir. Hay dos estructuras posibles para el
            valor de estas keys: 1) String que quiere decir que se incluirar todo lo que tenga el tweet en ese
            campo y 2) otro diccionario con el que filtrar los valores de la key del tweet
        """
        self.diccionarioParaFiltrar = diccionarioParaFiltrar

    def filtrarTweetjson(self, tweetjson):
        """
        Filtra un tweet en formato JSON utilizando el diccionario que se le ha pasado por parametro al objeto cuando
        se ha creado.

        :param tweetjson: Tweet en formato JSON que se quiere filtrar
        :return: El tweet en formato JSON filtrado. Si el diccionario y tweet no estan en formato
            dict, devuelve None
        """
        return self.filtrarTweetjsonConDiccionarioPorParametro(self.diccionarioParaFiltrar, tweetjson)

    def filtrarTweetjsonConDiccionarioPorParametro(self, diccionarioParaFiltrar, tweetjson):
        """
        Filtra un tweet en formato JSON o parte de este utilizando el diccionario que se le pasa parametro.
        Esta funcion es recursiva iterando sobre cada elemento del tweet hasta que se ha filtrado todo

        :param diccionarioParaFiltrar: diccionario con el que se filtra el tweet
        :param tweetjson: Tweet o parte del tweet otiginal en formato JSON que se quiere filtrar
        :return: El tweet o parte del tweet en formato JSON filtrado. Si el diccionario y tweet no estan en formato
            dict, devuelve None
        """

        if type(tweetjson).__name__ == "dict" and type(
                diccionarioParaFiltrar).__name__ == "dict":  # ambos tienen que ser en formato dict
            tweetjsonCopia = tweetjson.copy()  # Se copia el contenido y sera en este donde se vaya eliminando las keys

            for key in tweetjson.iterkeys():  # Se itera sobre los keys del json del tweet
                if diccionarioParaFiltrar.has_key(
                        key):  # El key esta en el diccionario por lo que no puede ser descartado
                    diccionarioParaFiltrarEsteKey = diccionarioParaFiltrar[key]

                    if type(
                            diccionarioParaFiltrarEsteKey).__name__ == "str":  # Si el valor del diccionario es del tipo str mantiene todos los valores
                        pass

                    elif type(
                            diccionarioParaFiltrarEsteKey).__name__ == "dict":  # Si el valor del diccionario es de nuevo de tipo dict, filtra el contenido

                        if type(tweetjson[
                                    key]).__name__ == "list":  # Los valores del json es una lista por lo que se tiene que filtrar cada uno por separado
                            listtweetjsonCopia = list()  # Contendra los valores filtrados de tweetjson[key]
                            for valorjson in tweetjson[key]:  # Filtra cada elemento en la lista
                                valorjsonfiltrado = self.filtrarTweetjsonConDiccionarioPorParametro(
                                    diccionarioParaFiltrarEsteKey, valorjson)
                                if valorjsonfiltrado:
                                    listtweetjsonCopia.append(valorjsonfiltrado)

                            tweetjsonCopia[key] = listtweetjsonCopia  # Pone los elementos filtrados en el JSON
                        elif type(tweetjson[
                                      key]).__name__ == "dict":  # Los valores del json es un dict por lo que se tiene que filtrar
                            valorjsonfiltrado = self.filtrarTweetjsonConDiccionarioPorParametro(
                                diccionarioParaFiltrarEsteKey, tweetjson[key])

                            if valorjsonfiltrado:  # Si es no None el valor se actualiza
                                tweetjsonCopia[key] = valorjsonfiltrado
                            else:  # Si es None se borra
                                del tweetjsonCopia[key]
                        else:  # Si el valor del JSON es de cualquier otro tipo lo elimina
                            del tweetjsonCopia[key]
                    else:  # Si el valor del diccionario es del otro tipo lo elimina
                        del tweetjsonCopia[key]
                else:  # El key no esta en el diccionario por lo que tiene que ser descartado
                    del tweetjsonCopia[key]
            return tweetjsonCopia
        else:
            return None


class TwiterListener(tweepy.StreamListener):
    """
    Listener de Twitter para escuchar sobre un determinado tema recibiendo tweets en streaming.

    Para evitar ser baneado, se puede poner un limite de tweets que se almacenan.

    A la hora del procesado, se mira que el tweet no sea retweet para evitar guardar tweets con el mismo texto y que
    ocupen espacio en disco. En otros contextos se podria estar interesados en guardarlos. Si es retweet, el limite
    no se ve afectado.

    Una vez que se procesa, se guarda en disco a traves de un escritor y se chequea si se ha alcanzado el limite o no.

    Si se pone un limite menor que 0, se estara escuchando indefinidamente.

    Cada objeto tendra un atributo forzarParo que indicara si el programa tiene que parar/terminar por un problema
    que no se puede recuperar
    """

    LIMITE = -1

    def __init__(self, escritorTweets, api, filtroTwiter, limite=LIMITE, numeroActualTweets=0,
                 guardarTweetsEnteros=False):
        """
        Crea el objeto

        :param escritorTweets: Clase que se encargara de escribir los tweets en disco
        :param api: tweepy.API con el autenticado
        :param limite: limite de numero de tweets no retweet que se desea. Por defecto es -1
        :param numeroActualTweets: numero actual que se han escrito en disco
        :param guardarTweetsEnteros: guarda los tweets enteros aparte en disco
        """
        self.escritorTweets = escritorTweets
        self.api = api
        self.filtroTwiter = filtroTwiter
        self.limite = limite
        self.numeroActualTweets = numeroActualTweets
        self.guardarTweetsEnteros = guardarTweetsEnteros
        self.forzarParo = False

    def on_connect(self):
        """
        Se conecta a la API de Twitter y se imprime por pantalla
        """
        print "Se crea conexion con Twitter"

    def on_data(self, dato):
        """
        Metodo que sera llamado cada vez que se obtenga un tweet.
        Si el tweet esta truncado, se obtiene el texto completo a traves de la api: api.get_status(datoJson["id"], tweet_mode="extended")
        Cada 50 tweets se imprime un mensaje por pantalla.

        :param dato: Tweet
        :return: True si se quiere continuar con la escucha y False en caso contrario

        :throws TwiterExcepcion: Si no se puede escribir en disco y no se puede recuperar
        :throws Exception: Si se produce otro error
        """

        # seTieneQueParar = super(TwiterListener, self).on_data(dato)
        datoJson = json.loads(dato)  # Se carga el dato en formato JSON

        if "text" in datoJson:
            if "retweeted_status" not in datoJson:  # Si es un retweet se elimina ya que contiene el mismo texto

                if "truncated" in datoJson and datoJson[
                    "truncated"]:  # Si el texto esta truncado se obtiene el texto completo
                    tweetExtendido = self.api.get_status(datoJson["id"], tweet_mode="extended")
                    if tweetExtendido and "full_text" in tweetExtendido._json:
                        datoJson["text"] = tweetExtendido._json["full_text"]

                datoJsonFiltrado = self.filtroTwiter.filtrarTweetjson(
                    datoJson)  # Se filtra el tweet y se queda con los datos en los que se este interesado

                if datoJsonFiltrado:  # Se escribe el tweet en disco y se mira si se ha llegado al limite

                    try:
                        if self.guardarTweetsEnteros:
                            self.escritorTweets.escribirTweet(datoJson)
                        self.escritorTweets.escribirTweetFiltrado(datoJsonFiltrado)
                        self.numeroActualTweets += 1

                        if self.numeroActualTweets % 50 == 0:
                            print "Se sigue escuchando"

                        if self.limite != -1 and self.numeroActualTweets >= self.limite:
                            self.forzarParo = True
                            return False

                    except util.TwiterExcepcion as e:  # Puede ser que no se pueda escribir el tweet o haya otro problema
                        if e.terminarPrograma:
                            self.forzarParo = True
                            raise e
                    except Exception as e:
                        raise e
        return True

    def on_error(self, status):
        print(status)

    def on_limit(self, track):
        self.forzarParo = True

    def on_error(self, status_code):
        self.forzarParo = True
        return False

    def on_timeout(self):
        self.forzarParo = True
        return

    def on_disconnect(self, notice):
        self.forzarParo = True


if __name__ == '__main__':
    """
    Si se llama a este programa, se parsea los parametros.
    
    Despues se crean los objetos que manejaran la escritura de tweets en disco (en esta ocasion en Mongodb) y el filtro
    para las keys de los tweets. Por ultimo se crea la conexion con la API y se empieza a escuchar.  
    """

    parser = argparse.ArgumentParser(description="Este programa permite conectarse a Twitter y almacenar los tweets")
    parser.add_argument("-ck", "--consumerkey", help="Consumer key")
    parser.add_argument("-cs", "--consumersecret", help="Consumer secret")
    parser.add_argument("-t", "--token", help="Token")
    parser.add_argument("-s", "--secret", help="Secret")

    parser.add_argument("-mdbh", "--mongodbhost", default="localhost", help="Mongodb host")
    parser.add_argument("-mdbp", "--mongodbpuerto", default=27017, type=int, help="Mongodb puerto")
    parser.add_argument("-mdbu", "--mongodbuser", help="Mongodb usuario")
    parser.add_argument("-mdbc", "--mongodbcontrasenya", help="Mongodb contrasenya")

    parser.add_argument("-gte", "--guardartweetsenteros", default=False, action='store_true',
                        help="Guarda los tweets sin ser procesados")
    parser.add_argument("-bat", "--borraranteriorestweets", default=False, action='store_true',
                        help="Borra los anteriores tweest almacenados")
    parser.add_argument("-lt", "--limitetweets", default=-1, type=int,
                        help="Limite de tweets que se escuchan y almacenan")
    parser.add_argument("-tt", "--temastweets", default=["madrid"], nargs='+',
                        help="Lista de temas en los que se esta interesado")

    args = parser.parse_args()

    # Se autentica usando los parametros pasado por parametro
    auth = tweepy.OAuthHandler(args.consumerkey, args.consumersecret)
    auth.set_access_token(args.token, args.secret)
    api = tweepy.API(auth)

    # Se crea el manejador de Mongodb con el que se obtendra la base de datos y las colecciones.
    # Tambien se crea es escritor de los tweets en Mongodb y el filtro de las keys de los tweets
    manejadorMongodb = util.ManejadorMongodb(mongodbHost=args.mongodbhost, mongodbPuerto=args.mongodbpuerto,
                                             usuario=args.mongodbuser, password=args.mongodbcontrasenya)
    mongodbEscritorTweets = util.MongodbEscritorTweets(manejadorMongodb,
                                                       vaciarAnterioresColecciones=args.borraranteriorestweets)
    filtroTwiter = FiltroTwiter(DICT_KEYS_TWEERS)

    numeroActualTweets = 0
    para = False
    # Puede ser que el listener lance alguna excepcion, por lo que se tiene que manejar.
    # Mientras que o bien no se tenga limite o no se haya alcanzado y no se tenga que parar, escucha.
    while (args.limitetweets < 0 or numeroActualTweets < args.limitetweets) and not para:
        try:
            twiterListener = TwiterListener(mongodbEscritorTweets, api, filtroTwiter, limite=args.limitetweets,
                                            numeroActualTweets=numeroActualTweets,
                                            guardarTweetsEnteros=args.guardartweetsenteros)
            stream = tweepy.Stream(auth, twiterListener)
            stream.filter(track=args.temastweets)

            numeroActualTweets = twiterListener.numeroActualTweets
            para = twiterListener.forzarParo
        except httplib.IncompleteRead:  # Hay un problema con la conexion por lo que lanza de nuevo el listener
            numeroActualTweets = twiterListener.numeroActualTweets
        except KeyboardInterrupt:  # Se ha detenido por el usuario, por lo que se tiene que salir
            stream.disconnect()
            numeroActualTweets = twiterListener.numeroActualTweets
            para = True
        except util.TwiterExcepcion as e:  # Se ha lanzado una excepcion del programa por lo que mira si se tiene que parar o no
            numeroActualTweets = twiterListener.numeroActualTweets
            para = twiterListener.forzarParo
        except Exception as e:  # Se ha lanzado una excepcion general, por lo que mira si se tiene que parar o no
            print e.message
            numeroActualTweets = twiterListener.numeroActualTweets
            para = twiterListener.forzarParo
