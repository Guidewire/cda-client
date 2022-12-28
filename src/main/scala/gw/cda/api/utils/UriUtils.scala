package gw.cda.api.utils

import java.net.URI
import java.nio.file.{Path, Paths}

/**
 * Utils for manipulating URIs with paths.
 */
object UriUtils {

  /**
   * Appends the given path to the URI.
   */
  def append(uri: URI, path: Path): URI = {
    val fullpath = Paths.get(uri).resolve(path)
    if ("file".equalsIgnoreCase(uri.getScheme) || uri.getScheme == null) {
      return prune(fullpath.toUri)
    }
    new URI(uri.getScheme, uri.getHost, fullpath.toString, null)
  }

  /**
   * Removes trailing slash from the URI, if present.
   */
  def prune(uri: URI): URI = {
    if (uri.getPath.length > 1 && (uri.getPath.endsWith("/"))) {
      return new URI(uri.getScheme, uri.getHost, uri.getPath.substring(0, uri.getPath.length - 1), uri.getFragment)
    }
    uri
  }

  /**
   * Replaces the scheme of the given URI.
   */
  def scheme(uri: URI, scheme: String): URI = {
    if (scheme.equalsIgnoreCase(uri.getScheme)) {
      return uri
    }
    new URI(scheme, null, null, null).resolve(uri)
  }
}
