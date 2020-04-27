# coding=utf-8

import pymongo
import pandas as pd
import dateutil.parser
import re
import string
from collections import Counter

__author__ = "Tatan Rufino"
__doc__ = """
Este fichero de Python contiene clases que se utilizaran a la hora de escuchar tweets, almacenarlos, parsearlos y en 
el analisis.
=====================================================================
Clases:
    * TwiterExcepcion: Excepcion general del programa. Hereda de Exception
    * UtilidadPatternTexto: Contiene la funcionalidad para parsear el texto. Entre las funcionalidades se puede
        encontrar: reemplazar emoticonos por texto, reemplazar menciones/hashtags/url en textos, contar
        numero de palabras, obtener todos los menciones/hashtags/urls de un texto...
    * ManejadorMongodb: Permite conectarse a Mongodb y obtener la base de datos y coleccions para almacenar los
        tweets.
    * EscritorTweets: Interfaz/clase que tendria que tener todas las clases que quieran escribir tweets en disco. 
        Actualmente se utiliza Mongodb pero podria crearse otra clase para escribir los tweest en un fichero
        heredando de esta clase.
    * MongodbEscritorTweets: Hereda EscritorTweets y permite escribir los tweets en Mongodb.
    * ParseadorTweetsAPandas: Interfaz/clase que tendria que tener todas las clases que quieran leer tweest desde
        el disco. Actualmente, como pasa con EscritorTweets, solo esta implementado para leer desde Mongodb.
    * MongodbParseadorTweetsAPandas: Hereda ParseadorTweetsAPandas y permite leer los tweets desde Mongodb y pasarlos
        a pandas.
    * AnalisisUtilidad: Utilidades para el analisis de los tweets una vez que se han almacenados. Se puede obtener 
        los elmentos totales en una serie pandas en los que cada elemento es una fila, asi como el numero de apariciones
        de elementos.

Testeado y versiones de librerias:
    * python 2.7.14
    * tweepy 3.5.0
    * pymongo 3.6.0
    * pandas 0.21.0
    * python-dateutil 2.6.1
"""


class TwiterExcepcion(Exception):
    """
    Clase con la exception customizada de este programa. Hereda de Exception. Tiene un atributo terminarPrograma que
    permite saber si el error es puntual o no se pude recuperar por lo que se tendria que terminar.
    """

    # Mensaje tipo que avisa que no se puede conectar a mongodb
    EXCEPTION_MENSAJE_NO_CONECTADO_MONGODB = "No se puede conectar a la base de datos. Comprueba que " \
                                             "esta levantada e intentelo de nuevo."
    # Mensaje tipo que avisa que el id esta duplicado en mondobd
    EXCEPTION_MENSAJE_ENTRADA_DUPLICADA_MONGODB = "El tweet ya esta almacenado."

    def __init__(self, mensaje, errores=None, terminarPrograma=False):
        """
        Crea la excepcion

        :param mensaje: Mensaje que se mostrara al usuario por pantalla
        :param errores: Mensaje extra sobre el/los errores
        :param terminarPrograma: True si el error es grave y False en caso contario
        """
        super(TwiterExcepcion, self).__init__(mensaje)
        self.mensaje = mensaje
        self.errores = errores
        self.terminarPrograma = terminarPrograma


class UtilidadPatternTexto(object):
    """
    Clase para tratar el texto. Para ello se utilizan pattern/regex.

    Metodos disponibles:
        * reemplazarEmoticonos: reemplaza todos los emoticonos en el texto
        * reemplazarMencionesHashtagsUrls: reemplaza todas las menciones, hashtags y urls en el texto
        * reemplazarSignospuntuacion: reemplaza todos los signos de puntacion dados por string.punctuation en el texto
        * obtenerEmoticonosEnTexto: se obtiene en una lista todos los emoticonos en el texto
        * obtenerMencionesEnTexto: se obtiene en una lista todas las menciones en el texto
        * obtenerHashtagsEnTexto: se obtiene en una lista todos los hashtags en el texto
        * obtenerUrlsEnTexto: se obtiene en una lista todas las urls en el texto
        * limpiarTexto: se limpia el texto, esto es, se eliminan todos los emoticonos, menciones, hashtags y urls, y
            se sustituyen todos los signos de puntuacion por espacios
        * contarNumeroPalabras: se cuentan el numero de palabras, esto es, caracteres seguidos y separados
            por espacio.
    """

    # Pattern para emoticonos. Son en formato unicode
    EMOTICONOS_PATTERN = re.compile(
        u"(\ud83d[\ude00-\ude4f])|"  # emoticonos
        u"(\ud83d[\u0000-\uddff])|"  # simbolos y pictogramas (2 de 2)
        u"(\ud83d[\ude80-\udeff])|"  # trasporte y mapas
        u"(\uD83E[\uDD00-\uDDFF])|"
        u"(\ud83c[\udf00-\uffff])|"  # simbolos & pictogramas (1 de 2)
        u"(\ud83c[\udde0-\uddff])|"  # flags (iOS)
        u"([\u2934\u2935]\uFE0F?)|"
        u"([\u3030\u303D]\uFE0F?)|"
        u"([\u3297\u3299]\uFE0F?)|"
        u"([\u203C\u2049]\uFE0F?)|"
        u"([\u00A9\u00AE]\uFE0F?)|"
        u"([\u2122\u2139]\uFE0F?)|"
        u"(\uD83C\uDC04\uFE0F?)|"
        u"(\uD83C\uDCCF\uFE0F?)|"
        u"([\u0023\u002A\u0030-\u0039]\uFE0F?\u20E3)|"
        u"(\u24C2\uFE0F?|[\u2B05-\u2B07\u2B1B\u2B1C\u2B50\u2B55]\uFE0F?)|"
        u"([\u2600-\u26FF]\uFE0F?)|"
        u"([\u2700-\u27BF]\uFE0F?)"
        "+", flags=re.UNICODE)

    MENCIONES_REGEX = r"(?:@[\w_]+)"  # Regex para menciones
    HASHTAGS_REGEX = r"(?:\#+[\w_]+[\w\'_\-]*[\w_]+)"  # Regex para hashtags
    URLS_REGEX = r"http[s]?://(?:[a-z]|[0-9]|[$-_@.&amp;+]|[!*\(\),]|(?:%[0-9a-f][0-9a-f]))+"  # Regex para urls

    MENCIONES_PATTERN = re.compile(MENCIONES_REGEX, re.VERBOSE | re.IGNORECASE)  # Pattern para menciones
    HASHTAGS_PATTERN = re.compile(HASHTAGS_REGEX, re.VERBOSE | re.IGNORECASE)  # Pattern para hashtags
    URLS_PATTERN = re.compile(URLS_REGEX, re.VERBOSE | re.IGNORECASE)  # Pattern para urls
    MENCIONES_HASHTAGS_URLS_PATTERN = re.compile(  # Pattern para menciones, hashtags y urls
        r"(" + MENCIONES_REGEX + "|" + HASHTAGS_REGEX + "|" + URLS_REGEX + ")", re.VERBOSE | re.IGNORECASE)
    SIGNOSPUNTUACION_PATTERN = re.compile('[%s]' % re.escape(string.punctuation))  # Pattern para signos de puntuacion

    def reemplazarEmoticonos(self, texto, caracter=""):
        """
        Reemplaza todos los emoticonos por los caracteres que se pasen por parametro.

        :param texto: Texto a ser parseado
        :param caracter: Caracter sustituto de cada emoticono
        :return: El texto con los emoticonos reemplazados
        """
        return UtilidadPatternTexto.EMOTICONOS_PATTERN.sub(r"" + caracter, texto) if texto else None

    def reemplazarMencionesHashtagsUrls(self, texto, caracter=""):
        """
        Reemplaza todas las menciones, hashtags y urls por los caracteres que se pasen por parametro.

        :param texto: Texto a ser parseado
        :param caracter: Caracter sustituto de cada mencion, hashtag y url
        :return: El texto con las enciones, hashtags y urls reemplazados
        """
        return UtilidadPatternTexto.MENCIONES_HASHTAGS_URLS_PATTERN.sub(r"" + caracter, texto) if texto else None

    def reemplazarSignospuntuacion(self, texto, caracter=""):
        """
        Reemplaza todos los signos de puntuacion por los caracteres que se pasen por parametro.

        :param texto: Texto a ser parseado
        :param caracter: Caracter sustituto de cada signo de puntuacion
        :return: El texto con los signos de puntuacion reemplazados
        """
        return UtilidadPatternTexto.SIGNOSPUNTUACION_PATTERN.sub(r"" + caracter, texto) if texto else None

    def obtenerEmoticonosEnTexto(self, texto):
        """
        Obtiene todos los emoticonos del texto.

        :param texto: Texto donde encontrar los emoticonos
        :return: Lista con todos los emoticonos del texto
        """
        return [token for linea in UtilidadPatternTexto.EMOTICONOS_PATTERN.findall(texto) for token in linea if
                token and len(token.strip()) > 0] if texto else []

    def obtenerMencionesEnTexto(self, texto):
        """
        Obtiene todas las menciones del texto.

        :param texto: Texto donde encontrar las menciones
        :return: Lista con todas las menciones del texto
        """
        return [linea for linea in UtilidadPatternTexto.MENCIONES_PATTERN.findall(texto)]

    def obtenerHashtagsEnTexto(self, texto):
        """
        Obtiene todos los hashtags del texto.

        :param texto: Texto donde encontrar los hashtags
        :return: Lista con todos los hashtags del texto
        """
        return [linea for linea in UtilidadPatternTexto.HASHTAGS_PATTERN.findall(texto)]

    def obtenerUrlsEnTexto(self, texto):
        """
        Obtiene todas las urls del texto.

        :param texto: Texto donde encontrar las urls
        :return: Lista con todas las urls del texto
        """
        return [linea for linea in UtilidadPatternTexto.URLS_PATTERN.findall(texto)]

    def limpiarTexto(self, texto):
        """
        Limpia el texto pasado por parametro: se reemplazan todos los emoticonos, menciones, hashtags, urls y signos
        de puntuacion por espacios; se eliminan todos los espacios por delante y por detras del texto; se sustituyen
        espacios seguidos por uno solo.

        :param texto: Texto a limpiar
        :return: Texto limpiado
        """
        textoSinEmoticonos = self.reemplazarEmoticonos(texto, caracter=" ")
        textoSinEmoticonosMencionesHashtagsUrls = self.reemplazarMencionesHashtagsUrls(textoSinEmoticonos, caracter=" ")
        textoSinEmoticonosMencionesHashtagsUrlsEspaciosSignospuntuacion = \
            self.reemplazarSignospuntuacion(textoSinEmoticonosMencionesHashtagsUrls, caracter=" ")
        textoSinEmoticonosMencionesHashtagsUrlsEspaciosSignospuntuacionUnEspacio = " ".join(
            textoSinEmoticonosMencionesHashtagsUrlsEspaciosSignospuntuacion.split()) if textoSinEmoticonosMencionesHashtagsUrlsEspaciosSignospuntuacion else ""
        return textoSinEmoticonosMencionesHashtagsUrlsEspaciosSignospuntuacionUnEspacio

    def contarNumeroPalabras(self, texto):
        """
        Cuenta el numero de palabras en el texto, esto es numero de secuencias de caracteres separados por espacios.

        :param texto: Texto del que se quiere obtener el numero de palabras
        :return: Numero de palabras en el texto
        """
        return len(texto.split()) if texto else 0


class ManejadorMongodb(object):
    """
    Clase para manejar Mongodb, esto es, conectarse y obtener las colecciones.

    Metodos disponibles:
        * obtenerColeccionTweets: obtiene la coleccion para guardar tweets no parseados
        * obtenerColeccionTweetsFiltrados: obtiene la coleccion para guardar tweets parseados
    """

    BASEDATOS_NOMBRE_TWEETS = "tweetsfinal"  # Nombre de la base de datos
    COLECCION_NOMBRE_TWEET = "tweet"  # Nombre de la coleccion para guardar tweets no parseados
    COLECCION_NOMBRE_TWEETFILTRADO = "tweetfiltrado"  # Nombre de la coleccion para guardar tweets parseados

    def __init__(self, mongodbHost, mongodbPuerto, usuario=None, password=None,
                 basedatosNombreTweets=BASEDATOS_NOMBRE_TWEETS, coleccionNombreTweet=COLECCION_NOMBRE_TWEET,
                 coleccionNombreTweetsFiltrado=COLECCION_NOMBRE_TWEETFILTRADO):
        """
        Crea el objeto para manejar el Mongodb. Lanzara una excepcion si no se puede conectar.

        :param mongodbHost: Mongodb host donde esta corriendo
        :param mongodbPuerto: Mongodb puerto donde esta escuchando
        :param usuario: Usuario para conectarse a Mongodb
        :param password: Password del usuario
        :param basedatosNombreTweets: Nombre de la base de datos para almacenar los tweets
        :param coleccionNombreTweet: Nombre de la coleccion para almacenar los tweets no parseados
        :param coleccionNombreTweetsFiltrado: Nombre de la coleccion para almacenar los tweets parseados
        """
        if usuario and password:
            self.mongoCliente = pymongo.MongoClient(
                'mongodb://%s:%s@%s:%d' % (usuario, password, mongodbHost, mongodbPuerto))
        else:
            self.mongoCliente = pymongo.MongoClient('mongodb://%s:%d' % (mongodbHost, mongodbPuerto))
        self.bbddTweets = self.mongoCliente[basedatosNombreTweets]
        self.coleccionNombreTweet = coleccionNombreTweet
        self.coleccionNombreTweetsFiltrado = coleccionNombreTweetsFiltrado

    def obtenerColeccionTweets(self):
        """
        Obtiene la coleccion para almacenar los tweest no parseados.

        :return: Coleccion para almacenar/leer los tweets no parseados
        """
        return self.bbddTweets[self.coleccionNombreTweet]

    def obtenerColeccionTweetsFiltrados(self):
        """
        Obtiene la coleccion para almacenar los tweest parseados.

        :return: Coleccion para almacenar/leer los tweets parseados
        """
        return self.bbddTweets[self.coleccionNombreTweetsFiltrado]


class EscritorTweets(object):
    """
    Interfaz que tendrian que heredar todas las clases que se quieran utilizar para escribir tweets en disco.

    Metodos disponibles:
        * escribirTweet: escribe un tweet en formato JSON no parseados
        * escribirTweetFiltrado: escribe un tweet en formato JSON parseado
        * borrarContenido: borra todos los tweest almacenados
    """

    def escribirTweet(self, tweetJson):
        """
        Escribe un tweet no parseado en disco.

        :param tweetJson: tweet en formato JSON no parseado para ser guardado
        """
        pass

    def escribirTweetFiltrado(self, tweetJson):
        """
        Escribe un tweet parseado en disco.

        :param tweetJson: tweet en formato JSON parseado para ser guardado
        """
        pass

    def borrarContenido(self):
        """
        Borra el contenido de ambas colecciones: tweets parseados y no parseados
        """
        pass


class MongodbEscritorTweets(EscritorTweets):
    """
    Clase para escribir tweets en Mongodb y que hereda de EscritorTweets. El _id del documento sera el id del tweet.

    Metodos disponibles:
        * escribirTweet: escribe un tweet en formato JSON no parseados
        * escribirTweetFiltrado: escribe un tweet en formato JSON parseado
        * escribir: escribe un tweet en una coleccion
        * borrarContenido: borra todos los tweest almacenados
        * ponerId: poner el id en el tweet JSON para ser utilizado como id del documento
    """

    def __init__(self, manejadorMongodb, vaciarAnterioresColecciones=False):
        """
        Crea el objeto para escribir tweets en Mongodb. Lanzara una excepcion si no se puede conectar.
        El _id del documento sera el id del tweet.

        :param manejadorMongodb: manejador de Mongodb para obtener las colecciones
        :param vaciarAnterioresColecciones: True si se quiere borrar todo el contenido, False en caso contrario
        """
        self.manejadorMongodb = manejadorMongodb
        if vaciarAnterioresColecciones:
            self.borrarContenido()

    def escribirTweet(self, tweetJson):
        """
        Escribe un tweet no parseado en Mongodb. El _id del documento sera el id del tweet.
        Lanzara una excepcion "leve" si el id esta repetido y una para terminar el programa si no se puede conectar.

        :param tweetJson: tweet en formato JSON no parseado para ser guardado
        """
        coleccionTweet = self.manejadorMongodb.obtenerColeccionTweets()
        self.escribir(tweetJson, coleccionTweet)

    def escribirTweetFiltrado(self, tweetJson):
        """
        Escribe un tweet parseado en Mongodb. El _id del documento sera el id del tweet.
        Lanzara una excepcion "leve" si el id esta repetido y una para terminar el programa si no se puede conectar.

        :param tweetJson: tweet en formato JSON parseado para ser guardado
        """
        coleccionTweet = self.manejadorMongodb.obtenerColeccionTweetsFiltrados()
        self.escribir(tweetJson, coleccionTweet)

    def escribir(self, tweetJson, coleccion):
        """
        Escribe un tweet parseado o no en Mongodb.
        Lanzara una excepcion "leve" si el id esta repetido y una para terminar el programa si no se puede conectar.

        :param tweetJson: tweet en formato JSON para ser guardado
        :param coleccion: coleccion de Mongodb donde ser almacenado el tweet
        """
        try:
            tweetJson = self.ponerId(tweetJson)
            coleccion.insert_one(tweetJson)
        except pymongo.errors.DuplicateKeyError:
            raise TwiterExcepcion(TwiterExcepcion.EXCEPTION_MENSAJE_ENTRADA_DUPLICADA_MONGODB, terminarPrograma=False)
        except pymongo.errors.ServerSelectionTimeoutError:
            raise TwiterExcepcion(TwiterExcepcion.EXCEPTION_MENSAJE_NO_CONECTADO_MONGODB, terminarPrograma=True)

    def borrarContenido(self):
        """
        Borra el contenido de ambas colecciones en Mongodb: tweets parseados y no parseados. Si hay algun problema
        lanzara una excepcion para terminar el programa.
        """
        try:
            coleccionTweet = self.manejadorMongodb.obtenerColeccionTweets()
            coleccionTweet.drop()
            coleccionTweet = self.manejadorMongodb.obtenerColeccionTweetsFiltrados()
            coleccionTweet.drop()
        except pymongo.errors.ServerSelectionTimeoutError:
            raise TwiterExcepcion(TwiterExcepcion.EXCEPTION_MENSAJE_NO_CONECTADO_MONGODB, terminarPrograma=True)

    def ponerId(self, tweetJson):
        """
        Se crea un nuevo campo en el tweet llamado _id con el valor del id del tweet para que sea utilizado como id
        del documento.

        :param tweetJson: tweet en formato JSON con el id
        :return: el tweet con un nuevo campo _id
        """
        if "id" in tweetJson:
            tweetJson["_id"] = tweetJson["id"]
        return tweetJson


class ParseadorTweetsAPandas(object):
    """
    Interfaz que tendrian que heredar todas las clases que se quieran utilizar para leer tweets en disco.
    Solo se utilizaran los tweets parseados.

    Metodos disponibles:
        * pasearTodosTweetsFiltradoEnPandas: parsea todos los tweets almacenados y los convierte en pandas.
            Se obtiene usuarios, texto, fecha de creacion, localizacion del texto (no del usuario) y lenguaje.
        * anyadirHoraMinuto: anyade la hora y el minuto al pandas en distintas columnas
        * anyadirEmoticonosHashtagsMenciones: anyade los emoticonos, hashtags y menciones del texto en pandas a
            partir del texto. Estos seran listas de emoticonos, hashtags y menciones.
    """

    # Nombre de las columnas del pandas
    NOMBRE_COLUMNA_USUARIO = "usuario"
    NOMBRE_COLUMNA_FECHACREACION = "fecha_creacion"
    NOMBRE_COLUMNA_TEXTO = "texto"
    NOMBRE_COLUMNA_LOCALIZACION = "localizacion"
    NOMBRE_COLUMNA_NUMEROPALABRAS = "numero_palabras"
    NOMBRE_COLUMNA_NUMEROCARACTERES = "numero_caracteres"
    NOMBRE_COLUMNA_HORA = "hora"
    NOMBRE_COLUMNA_MINUTO = "minuto"
    NOMBRE_COLUMNA_LENGUAJE = "lenguaje"
    NOMBRE_COLUMNA_EMOTICONOS = "emoticonos"
    NOMBRE_COLUMNA_HASHTAGS = "hashtags"
    NOMBRE_COLUMNA_MENCIONES = "menciones"

    def pasearTodosTweetsFiltradoEnPandas(self):
        """
        Parsea todos los tweets almacenados y los convierte en pandas.
        Se obtiene usuarios, texto, fecha de creacion, localizacion del texto (no del usuario) y lenguaje
        Se crea un nuevo campo en el tweet llamado _id con el valor del id del tweet para que sea utilizado como id
        del documento.

        Ademas, se anyade el numero de caracteres y palabras en el texto. Para ello, se eliminan todos los emoticonos,
        menciones, hashtags y urls, se calcula el numero de palabras en este texto y luego se anyade por cada
        emoticonos, mencion, hashtag y url.

        El pandas se guarda en la variable local pdTweetsFiltrado.
        """
        pass

    def anyadirHoraMinuto(self):
        """
        Anyade la hora del tweet y el minuto de la creacion del tweet en el pandas pdTweetsFiltrado.
        """
        if len(self.pdTweetsFiltrado) > 0:
            self.pdTweetsFiltrado[ParseadorTweetsAPandas.NOMBRE_COLUMNA_HORA] = self.pdTweetsFiltrado[
                ParseadorTweetsAPandas.NOMBRE_COLUMNA_FECHACREACION].apply(
                lambda fecha: dateutil.parser.parse(fecha).hour if fecha else None)

            self.pdTweetsFiltrado[ParseadorTweetsAPandas.NOMBRE_COLUMNA_MINUTO] = self.pdTweetsFiltrado[
                ParseadorTweetsAPandas.NOMBRE_COLUMNA_FECHACREACION].apply(
                lambda fecha: dateutil.parser.parse(fecha).minute if fecha else None)

    def anyadirEmoticonosHashtagsMenciones(self):
        """
        Anyade la los emoticonos, hashtags y menciones que contiene el texto en el pandas pdTweetsFiltrado. Estos son
        listas.
        """
        if len(self.pdTweetsFiltrado) > 0:
            utilidadPatternTexto = UtilidadPatternTexto()

            self.pdTweetsFiltrado[ParseadorTweetsAPandas.NOMBRE_COLUMNA_EMOTICONOS] = self.pdTweetsFiltrado[
                ParseadorTweetsAPandas.NOMBRE_COLUMNA_TEXTO].apply(
                lambda texto: utilidadPatternTexto.obtenerEmoticonosEnTexto(texto) if texto in texto else [])

            self.pdTweetsFiltrado[ParseadorTweetsAPandas.NOMBRE_COLUMNA_HASHTAGS] = self.pdTweetsFiltrado[
                ParseadorTweetsAPandas.NOMBRE_COLUMNA_TEXTO].apply(
                lambda texto: utilidadPatternTexto.obtenerHashtagsEnTexto(texto) if texto in texto else [])

            self.pdTweetsFiltrado[ParseadorTweetsAPandas.NOMBRE_COLUMNA_MENCIONES] = self.pdTweetsFiltrado[
                ParseadorTweetsAPandas.NOMBRE_COLUMNA_TEXTO].apply(
                lambda texto: utilidadPatternTexto.obtenerMencionesEnTexto(texto) if texto in texto else [])


class MongodbParseadorTweetsAPandas(ParseadorTweetsAPandas):
    """
    Clase que hereda de ParseadorTweetsAPandas y que lee los tweets parseados en Mongodb y los convierte en panda.

    Metodos disponibles:
        * pasearTodosTweetsFiltradoEnPandas: parsea todos los tweets almacenados en Mongodb y los convierte en pandas.
        * parsearTweet: parsea un tweet individual y lo covierte un diccionario con los key-valores del panda.
    """

    def __init__(self, manejadorMongodb):
        """
        Crea el objeto para convertir los tweets parseados almacenados en Mongodb (JSON) en pandas

        :param manejadorMongodb: manejador de Mongodb para obtener las colecciones
        """
        self.manejadorMongodb = manejadorMongodb
        self.pdTweetsFiltrado = pd.DataFrame()

    def pasearTodosTweetsFiltradoEnPandas(self):
        """
        Lee todos los tweets parseados almacenados en Mongodb y los convierte en Pandas. Se almacenara en la
        variable del objeto pdTweetsFiltrado.
        Lanzara una excepcion para terminar el programa si no se puede conectar a Mongodb.
        """
        try:
            tweets = self.manejadorMongodb.obtenerColeccionTweetsFiltrados().find({})
            for tweet in tweets:
                self.pdTweetsFiltrado = self.pdTweetsFiltrado.append(
                    pd.Series(self.parsearTweet(tweet), name=tweet["id_str"]))
        except pymongo.errors.ServerSelectionTimeoutError:
            raise TwiterExcepcion(TwiterExcepcion.EXCEPTION_MENSAJE_NO_CONECTADO_MONGODB, terminarPrograma=True)

    def parsearTweet(self, tweet):
        """
        Pasa a diccionario el tweet para ser almacenado en pandas.

        Ademas, se anyade el numero de caracteres y palabras en el texto. Para ello, se eliminan todos los emoticonos,
        menciones, hashtags y urls, se calcula el numero de palabras en este texto y luego se anyade por cada
        emoticonos, mencion, hashtag y url. Para obtener el numer de caracteres se eliman los emoticonos, se obtiene
        la longitud y luego se anyade la cantidad de emoticonos en el texto.

        :param tweet: tweet en formato JSON para convertir en un diccionario para ser almacenado en pandas
        :return: diccionario con las keys de los nombres de columnas del panda y los valores del tweet.
        """
        tweetEnPdFormato = dict()
        tweetEnPdFormato[ParseadorTweetsAPandas.NOMBRE_COLUMNA_USUARIO] = tweet["user"][
            "name"] if "user" in tweet and "name" in tweet["user"] else None
        tweetEnPdFormato[ParseadorTweetsAPandas.NOMBRE_COLUMNA_FECHACREACION] = tweet[
            "created_at"] if "created_at" in tweet else None
        tweetEnPdFormato[ParseadorTweetsAPandas.NOMBRE_COLUMNA_TEXTO] = tweet["text"] if "text" in tweet else None
        tweetEnPdFormato[ParseadorTweetsAPandas.NOMBRE_COLUMNA_LOCALIZACION] = tweet["place"][
            "full_name"] if "place" in tweet and tweet["place"] and "full_name" in tweet["place"] else None

        numeroCaracteres = 0
        numeroPalabras = 0
        if "text" in tweet:  # Si hay texto en el tweet
            texto = tweet["text"]
            utilidadPatternTexto = UtilidadPatternTexto()
            textoSinEmoticonos = utilidadPatternTexto.reemplazarEmoticonos(texto)
            emoticonosEnElTexto = utilidadPatternTexto.obtenerEmoticonosEnTexto(texto)
            numeroCaracteres = len(textoSinEmoticonos) + len(emoticonosEnElTexto) if textoSinEmoticonos else len(texto)
            numeroPalabras = utilidadPatternTexto.contarNumeroPalabras(utilidadPatternTexto.limpiarTexto(texto))

        tweetEnPdFormato[ParseadorTweetsAPandas.NOMBRE_COLUMNA_NUMEROCARACTERES] = numeroCaracteres
        tweetEnPdFormato[ParseadorTweetsAPandas.NOMBRE_COLUMNA_NUMEROPALABRAS] = numeroPalabras

        tweetEnPdFormato[ParseadorTweetsAPandas.NOMBRE_COLUMNA_LENGUAJE] = tweet["lang"] if "lang" in tweet else None

        return tweetEnPdFormato


class AnalisisUtilidad(object):
    """
    Clase que se utiliza para el analisis de los tweets almacenados: se puede obtener los elementos mas comunes
    en una serie pandas (columna en data frame) y su frecuencia, y el numero total de elementos.

    Metodos disponibles:
        * obtenerNumeroDeElementosListaEnSeriePandas: obtiene el numero total de elementos de una serie pandas
            donde cada fila esta compuesta por una lista.
        * obtenerContadorDeElementosListaEnSeriePandas: obtiene el numero total de apariciones de cada elemento
            de una serie pandas donde cada fila esta compuesta por una lista. Se utiliza collections.Counter.
        * obtenerContadorDeElementosNoListaEnSeriePandas: obtiene el numero total de apariciones de cada elemento
            de una serie pandas donde cada fila esta compuesta un unico elemento (no lista).
            Se utiliza collections.Counter.

    """

    def obtenerNumeroDeElementosListaEnSeriePandas(self, seriePandas, promedio=False):
        """
        Obtiene el numero total de elementos de una serie pandas donde cada fila esta compuesta por una lista.

        :param seriePandas: serie pandas del que se quiere obtener el numero total de elementos. Los elementos en
            esta serie tiene que ser una lista.
        :param promedio: True si se quiere obtener el numero promedio o False si se quiere obtener el numero total.
            Por defecto es False.
        :return: numero de elementos totales en esta serie. El numero de apariciones puede ser total si promedio
            es False o la media si es True.
        """
        numeroTotal = 0
        for valoresFila in seriePandas:
            if valoresFila:
                numeroTotal += len(valoresFila)
        if promedio and len(seriePandas) > 0:
            numeroTotal /= float(len(seriePandas))
        return numeroTotal

    def obtenerContadorDeElementosListaEnSeriePandas(self, seriePandas, promedio=False, top=None):
        """
        Obtiene el numero total de apariciones de cada elemento de una serie pandas donde cada fila esta
        compuesta por una lista. Se utiliza collections.Counter.

        :param seriePandas: serie pandas del que se quiere obtener el contador de elementos. Los elementos en
            esta serie tiene que ser una lista.
        :param promedio: True si se quiere obtener el numero promedio de apariciones de cada elemento o False
            si se quiere obtener el numero total. Por defecto es False.
        :param top: None si se quiere obtener todos los elementos o un numero para obtener los top primeros.
            Por defecto es None.
        :return: elementos mas comunes que se repiten. Si top es None, se devolvera todos los elementos sino se
            devolvera los tops primeros mas frecuentes. El numero de apariciones puede ser total si promedio
            es False o la media si es True.
        """
        contador = Counter()
        for valoresFila in seriePandas:
            if valoresFila:
                for valorFila in valoresFila:
                    contador[valorFila] += 1
        if promedio and len(seriePandas) > 0:
            for elemento in contador:
                contador[elemento] /= float(len(seriePandas))
        if top:  # Si se ha pasado un top se devuelven solo los top que se desea
            return contador.most_common(top)
        else:  # Se devuelve el contador completo
            return contador.most_common()

    def obtenerContadorDeElementosNoListaEnSeriePandas(self, seriePandas, promedio=False, top=None):
        """
        Obtiene el numero total de apariciones de cada elemento de una serie pandas donde cada fila esta
        compuesta por un unico elemento. Se utiliza collections.Counter.

        :param seriePandas: serie pandas del que se quiere obtener el contador de elementos. Los elementos en
            esta serie no tiene que ser lista.
        :param promedio: True si se quiere obtener el numero promedio de apariciones de cada elemento o False
            si se quiere obtener el numero total. Por defecto es False.
        :param top: None si se quiere obtener todos los elementos o un numero para obtener los top primeros.
            Por defecto es None.
        :return: elementos mas comunes que se repiten. Si top es None, se devolvera todos los elementos sino se
            devolvera los tops primeros mas frecuentes. El numero de apariciones puede ser total si promedio
            es False o la media si es True.
        """
        contador = Counter()
        for valorFila in seriePandas:
            if valorFila:
                contador[valorFila] += 1
        if promedio and len(seriePandas) > 0:
            for elemento in contador:
                contador[elemento] /= float(len(seriePandas))
        if top:  # Si se ha pasado un top se devuelven solo los top que se desea
            return contador.most_common(top)
        else:  # Se devuelve el contador completo
            return contador.most_common()
