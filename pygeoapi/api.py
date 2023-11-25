# =================================================================
#
# Authors: Tom Kralidis <tomkralidis@gmail.com>
#          Francesco Bartoli <xbartolone@gmail.com>
#          Sander Schaminee <sander.schaminee@geocat.net>
#          John A Stevenson <jostev@bgs.ac.uk>
#          Colin Blackburn <colb@bgs.ac.uk>
#
# Copyright (c) 2022 Tom Kralidis
# Copyright (c) 2020 Francesco Bartoli
# Copyright (c) 2022 John A Stevenson and Colin Blackburn
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# =================================================================
""" Root level code of pygeoapi, parsing content provided by web framework.
Returns content from plugins and sets responses.
"""

import asyncio
from collections import OrderedDict
from copy import deepcopy
from datetime import datetime, timezone
from functools import partial
from gzip import compress
import json
import logging
import os
import re
from typing import Any, Tuple, Union
import urllib.parse

from uuid import UUID
from dateutil.parser import parse as dateparse
from pygeofilter.parsers.ecql import parse as parse_ecql_text
from pygeofilter.parsers.cql_json import parse as parse_cql_json
import pytz
from shapely.errors import WKTReadingError
from shapely.wkt import loads as shapely_loads

from pygeoapi import __version__, l10n
from pygeoapi.formatter.base import FormatterSerializationError
from pygeoapi.linked_data import (geojson2jsonld, jsonldify,
                                jsonldify_collection)
from pygeoapi.log import setup_logger
from pygeoapi.process.base import ProcessorExecuteError
from pygeoapi.plugin import load_plugin, PLUGINS
from pygeoapi.provider.base import (
    ProviderGenericError, ProviderConnectionError, ProviderNotFoundError,
    ProviderInvalidDataError, ProviderInvalidQueryError, ProviderNoDataError,
    ProviderQueryError, ProviderItemNotFoundError, ProviderTypeError)

from pygeoapi.provider.tile import (ProviderTileNotFoundError,
                                    ProviderTileQueryError,
                                    ProviderTilesetIdNotFoundError)
from pygeoapi.models.cql import CQLModel
from pygeoapi.models.process_data import ProcessMobilityData
from pygeoapi.util import (dategetter, DATETIME_FORMAT,
                        filter_dict_by_key_value, get_provider_by_type,
                        get_provider_default, get_typed_value, JobStatus,
                        json_serial, render_j2_template, str2bool,
                        TEMPLATES, to_json)
import pymeos
import click
import psycopg2
from pymeos import *

LOGGER = logging.getLogger(__name__)

#: Return headers for requests (e.g:X-Powered-By)
HEADERS = {
    'Content-Type': 'application/json',
    'X-Powered-By': 'pygeoapi {}'.format(__version__)
}

CHARSET = ['utf-8']
F_JSON = 'json'
F_HTML = 'html'
F_JSONLD = 'jsonld'
F_GZIP = 'gzip'

#: Formats allowed for ?f= requests (order matters for complex MIME types)
FORMAT_TYPES = OrderedDict((
    (F_HTML, 'text/html'),
    (F_JSONLD, 'application/ld+json'),
    (F_JSON, 'application/json'),
))

#: Locale used for system responses (e.g. exceptions)
SYSTEM_LOCALE = l10n.Locale('en', 'US')

CONFORMANCE = {
    'common': [
        'http://www.opengis.net/spec/ogcapi-common-1/1.0/conf/core',
        'http://www.opengis.net/spec/ogcapi-common-2/1.0/conf/collections'
    ],
    'feature': [
        'http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/core',
        'http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/oas30',
        'http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/html',
        'http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/geojson',
        'http://www.opengis.net/spec/ogcapi-features-4/1.0/conf/create-replace-delete'  # noqa
    ],
    'coverage': [
        'http://www.opengis.net/spec/ogcapi-coverages-1/1.0/conf/core',
        'http://www.opengis.net/spec/ogcapi-coverages-1/1.0/conf/oas30',
        'http://www.opengis.net/spec/ogcapi-coverages-1/1.0/conf/html',
        'http://www.opengis.net/spec/ogcapi-coverages-1/1.0/conf/geodata-coverage',  # noqa
        'http://www.opengis.net/spec/ogcapi-coverages-1/1.0/conf/coverage-subset',  # noqa
        'http://www.opengis.net/spec/ogcapi-coverages-1/1.0/conf/coverage-rangesubset',  # noqa
        'http://www.opengis.net/spec/ogcapi-coverages-1/1.0/conf/coverage-bbox',  # noqa
        'http://www.opengis.net/spec/ogcapi-coverages-1/1.0/conf/coverage-datetime'  # noqa
    ],
    'tile': [
        'http://www.opengis.net/spec/ogcapi-tiles-1/1.0/conf/core'
    ],
    'record': [
        'http://www.opengis.net/spec/ogcapi-records-1/1.0/conf/core',
        'http://www.opengis.net/spec/ogcapi-records-1/1.0/conf/sorting',
        'http://www.opengis.net/spec/ogcapi-records-1/1.0/conf/opensearch',
        'http://www.opengis.net/spec/ogcapi-records-1/1.0/conf/json',
        'http://www.opengis.net/spec/ogcapi-records-1/1.0/conf/html'
    ],
    'process': [
        'http://www.opengis.net/spec/ogcapi-processes-1/1.0/conf/ogc-process-description', # noqa
        'http://www.opengis.net/spec/ogcapi-processes-1/1.0/conf/core',
        'http://www.opengis.net/spec/ogcapi-processes-1/1.0/conf/json',
        'http://www.opengis.net/spec/ogcapi-processes-1/1.0/conf/oas30'
    ],
    'edr': [
        'http://www.opengis.net/spec/ogcapi-edr-1/1.0/conf/core'
    ],
    'movingfeatures': [
        "http://www.opengis.net/spec/ogcapi-movingfeatures-1/1.0/conf/common",
        "http://www.opengis.net/spec/ogcapi-movingfeatures-1/1.0/conf/mf-collection",
        "http://www.opengis.net/spec/ogcapi-movingfeatures-1/1.0/conf/movingfeatures"
    ]
}

OGC_RELTYPES_BASE = 'http://www.opengis.net/def/rel/ogc/1.0'


def pre_process(func):
    """
    Decorator that transforms an incoming Request instance specific to the
    web framework (i.e. Flask or Starlette) into a generic :class:`APIRequest`
    instance.

    :param func: decorated function

    :returns: `func`
    """

    def inner(*args):
        cls, req_in = args[:2]
        req_out = APIRequest.with_data(req_in, getattr(cls, 'locales', set()))
        if len(args) > 2:
            return func(cls, req_out, *args[2:])
        else:
            return func(cls, req_out)

    return inner


def gzip(func):
    """
    Decorator that compresses the content of an outgoing API result
    instance if the Content-Encoding response header was set to gzip.

    :param func: decorated function

    :returns: `func`
    """

    def inner(*args, **kwargs):
        headers, status, content = func(*args, **kwargs)
        if F_GZIP in headers.get('Content-Encoding', []):
            try:
                charset = CHARSET[0]
                headers['Content-Type'] = \
                    f"{headers['Content-Type']}; charset={charset}"
                content = compress(content.encode(charset))
            except TypeError as err:
                headers.pop('Content-Encoding')
                LOGGER.error('Error in compression: {}'.format(err))

        return headers, status, content

    return inner


class APIRequest:
    """
    Transforms an incoming server-specific Request into an object
    with some generic helper methods and properties.

    .. note::   Typically, this instance is created automatically by the
                :func:`pre_process` decorator. **Every** API method that has
                been routed to a REST endpoint should be decorated by the
                :func:`pre_process` function.
                Therefore, **all** routed API methods should at least have 1
                argument that holds the (transformed) request.

    The following example API method will:

    - transform the incoming Flask/Starlette `Request` into an `APIRequest`
      using the :func:`pre_process` decorator;
    - call :meth:`is_valid` to check if the incoming request was valid, i.e.
      that the user requested a valid output format or no format at all
      (which means the default format);
    - call :meth:`API.get_format_exception` if the requested format was
      invalid;
    - create a `dict` with the appropriate `Content-Type` header for the
      requested format and a `Content-Language` header if any specific language
      was requested.

    .. code-block:: python

       @pre_process
       def example_method(self, request: Union[APIRequest, Any], custom_arg):
          if not request.is_valid():
             return self.get_format_exception(request)

          headers = request.get_response_headers()

          # generate response_body here

          return headers, 200, response_body


    The following example API method is similar as the one above, but will also
    allow the user to request a non-standard format (e.g. ``f=xml``).
    If `xml` was requested, we set the `Content-Type` ourselves. For the
    standard formats, the `APIRequest` object sets the `Content-Type`.

    .. code-block:: python

       @pre_process
       def example_method(self, request: Union[APIRequest, Any], custom_arg):
          if not request.is_valid(['xml']):
             return self.get_format_exception(request)

          content_type = 'application/xml' if request.format == 'xml' else None
          headers = request.get_response_headers(content_type)

          # generate response_body here

          return headers, 200, response_body

    Note that you don't *have* to call :meth:`is_valid`, but that you can also
    perform a custom check on the requested output format by looking at the
    :attr:`format` property.
    Other query parameters are available through the :attr:`params` property as
    a `dict`. The request body is available through the :attr:`data` property.

    .. note::   If the request data (body) is important, **always** create a
                new `APIRequest` instance using the :meth:`with_data` factory
                method.
                The :func:`pre_process` decorator will use this automatically.

    :param request:             The web platform specific Request instance.
    :param supported_locales:   List or set of supported Locale instances.
    """
    def __init__(self, request, supported_locales):
        
        # Set default request data
        self._data = b''

        # Copy request query parameters
        self._args = self._get_params(request)
        self._method = getattr(request, "method", None)

        # Get path info
        if hasattr(request, 'scope'):
            self._path_info = request.scope['path'].strip('/')
        elif hasattr(request.headers, 'environ'):
            self._path_info = request.headers.environ['PATH_INFO'].strip('/')
        elif hasattr(request, 'path_info'):
            self._path_info = request.path_info

        # Extract locale from params or headers
        self._raw_locale, self._locale = self._get_locale(request.headers,
                                                          supported_locales)

        # Determine format
        self._format = self._get_format(request.headers)

        # Get received headers
        self._headers = self.get_request_headers(request.headers)

    @classmethod
    def with_data(cls, request, supported_locales) -> 'APIRequest':
        """
        Factory class method to create an `APIRequest` instance with data.

        If the request body is required, an `APIRequest` should always be
        instantiated using this class method. The reason for this is, that the
        Starlette request body needs to be awaited (async), which cannot be
        achieved in the :meth:`__init__` method of the `APIRequest`.
        However, `APIRequest` can still be initialized using :meth:`__init__`,
        but then the :attr:`data` property value will always be empty.

        :param request:             The web platform specific Request instance.
        :param supported_locales:   List or set of supported Locale instances.
        :returns:                   An `APIRequest` instance with data.
        """

        api_req = cls(request, supported_locales)
        if hasattr(request, 'data'):
            # Set data from Flask request
            api_req._data = request.data
        elif hasattr(request, 'body'):
            if 'django' in str(request.__class__):
                # Set data from Django request
                api_req._data = request.body
            else:
                try:
                    import nest_asyncio
                    nest_asyncio.apply()
                    # Set data from Starlette request after async
                    # coroutine completion
                    # TODO:
                    # this now blocks, but once Flask v2 with async support
                    # has been implemented, with_data() can become async too
                    loop = asyncio.get_event_loop()
                    api_req._data = loop.run_until_complete(request.body())
                except ModuleNotFoundError:
                    LOGGER.error('Module nest-asyncio not found')
        return api_req

    @staticmethod
    def _get_params(request):
        """
        Extracts the query parameters from the `Request` object.

        :param request: A Flask or Starlette Request instance
        :returns: `ImmutableMultiDict` or empty `dict`
        """

        if hasattr(request, 'args'):
            # Return ImmutableMultiDict from Flask request
            return request.args
        elif hasattr(request, 'query_params'):
            # Return ImmutableMultiDict from Starlette request
            return request.query_params
        elif hasattr(request, 'GET'):
            # Return QueryDict from Django GET request
            return request.GET
        elif hasattr(request, 'POST'):
            # Return QueryDict from Django GET request
            return request.POST
        LOGGER.debug('No query parameters found')
        return {}

    def _get_locale(self, headers, supported_locales):
        """
        Detects locale from "lang=<language>" param or `Accept-Language`
        header. Returns a tuple of (raw, locale) if found in params or headers.
        Returns a tuple of (raw default, default locale) if not found.

        :param headers: A dict with Request headers
        :param supported_locales: List or set of supported Locale instances
        :returns: A tuple of (str, Locale)
        """

        raw = None
        try:
            default_locale = l10n.str2locale(supported_locales[0])
        except (TypeError, IndexError, l10n.LocaleError) as err:
            # This should normally not happen, since the API class already
            # loads the supported languages from the config, which raises
            # a LocaleError if any of these languages are invalid.
            LOGGER.error(err)
            raise ValueError(f"{self.__class__.__name__} must be initialized"
                             f"with a list of valid supported locales")

        for func, mapping in ((l10n.locale_from_params, self._args),
                              (l10n.locale_from_headers, headers)):
            loc_str = func(mapping)
            if loc_str:
                if not raw:
                    # This is the first-found locale string: set as raw
                    raw = loc_str
                # Check if locale string is a good match for the UI
                loc = l10n.best_match(loc_str, supported_locales)
                is_override = func is l10n.locale_from_params
                if loc != default_locale or is_override:
                    return raw, loc

        return raw, default_locale

    def _get_format(self, headers) -> Union[str, None]:
        """
        Get `Request` format type from query parameters or headers.

        :param headers: Dict of Request headers
        :returns: format value or None if not found/specified
        """

        # Optional f=html or f=json query param
        # Overrides Accept header and might differ from FORMAT_TYPES
        format_ = (self._args.get('f') or '').strip()
        if format_:
            return format_

        # Format not specified: get from Accept headers (MIME types)
        # e.g. format_ = 'text/html'
        h = headers.get('accept', headers.get('Accept', '')).strip() # noqa
        (fmts, mimes) = zip(*FORMAT_TYPES.items())
        # basic support for complex types (i.e. with "q=0.x")
        for type_ in (t.split(';')[0].strip() for t in h.split(',') if t):
            if type_ in mimes:
                idx_ = mimes.index(type_)
                format_ = fmts[idx_]
                break

        return format_ or None

    @property
    def data(self) -> bytes:
        """Returns the additional data send with the Request (bytes)"""
        return self._data

    @property
    def params(self) -> dict:
        """Returns the Request query parameters dict"""
        return self._args

    @property
    def path_info(self) -> str:
        """Returns the web server request path info part"""
        return self._path_info

    @property
    def locale(self) -> l10n.Locale:
        """
        Returns the user-defined locale from the request object.
        If no locale has been defined or if it is invalid,
        the default server locale is returned.

        .. note::   The locale here determines the language in which pygeoapi
                    should return its responses. This may not be the language
                    that the user requested. It may also not be the language
                    that is supported by a collection provider, for example.
                    For this reason, you should pass the `raw_locale` property
                    to the :func:`l10n.get_plugin_locale` function, so that
                    the best match for the provider can be determined.

        :returns: babel.core.Locale
        """

        return self._locale

    @property
    def raw_locale(self) -> Union[str, None]:
        """
        Returns the raw locale string from the `Request` object.
        If no "lang" query parameter or `Accept-Language` header was found,
        `None` is returned.
        Pass this value to the :func:`l10n.get_plugin_locale` function to let
        the provider determine a best match for the locale, which may be
        different from the locale used by pygeoapi's UI.

        :returns: a locale string or None
        """

        return self._raw_locale

    @property
    def format(self) -> Union[str, None]:
        """
        Returns the content type format from the
        request query parameters or headers.

        :returns: Format name or None
        """

        return self._format

    @property
    def headers(self) -> dict:
        """
        Returns the dictionary of the headers from
        the request.

        :returns: Request headers dictionary
        """

        return self._headers

    def get_linkrel(self, format_: str) -> str:
        """
        Returns the hyperlink relationship (rel) attribute value for
        the given API format string.

        The string is compared against the request format and if it matches,
        the value 'self' is returned. Otherwise, 'alternate' is returned.
        However, if `format_` is 'json' and *no* request format was found,
        the relationship 'self' is returned as well (JSON is the default).

        :param format_: The format to compare the request format against.
        :returns: A string 'self' or 'alternate'.
        """

        fmt = format_.lower()
        if fmt == self._format or (fmt == F_JSON and not self._format):
            return 'self'
        return 'alternate'

    def is_valid(self, additional_formats=None) -> bool:
        """
        Returns True if:
            - the format is not set (None)
            - the requested format is supported
            - the requested format exists in a list if additional formats

        .. note::   Format names are matched in a case-insensitive manner.

        :param additional_formats: Optional additional supported formats list

        :returns: bool
        """

        if not self._format:
            return True
        if self._format in FORMAT_TYPES.keys():
            return True
        if self._format in (f.lower() for f in (additional_formats or ())):
            return True
        return False

    def get_response_headers(self, force_lang: l10n.Locale = None,
                             force_type: str = None,
                             force_encoding: str = None) -> dict:
        """
        Prepares and returns a dictionary with Response object headers.

        This method always adds a 'Content-Language' header, where the value
        is determined by the 'lang' query parameter or 'Accept-Language'
        header from the request.
        If no language was requested, the default pygeoapi language is used,
        unless a `force_lang` override was specified (see notes below).

        A 'Content-Type' header is also always added to the response.
        If the user does not specify `force_type`, the header is based on
        the `format` APIRequest property. If that is invalid, the default MIME
        type `application/json` is used.

        ..note::    If a `force_lang` override is applied, that language
                    is always set as the 'Content-Language', regardless of
                    a 'lang' query parameter or 'Accept-Language' header.
                    If an API response always needs to be in the same
                    language, 'force_lang' should be set to that language.

        :param force_lang: An optional Content-Language header override.
        :param force_type: An optional Content-Type header override.
        :param force_encoding: An optional Content-Encoding header override.
        :returns: A header dict
        """

        headers = HEADERS.copy()
        l10n.set_response_language(headers, force_lang or self._locale)
        if force_type:
            # Set custom MIME type if specified
            headers['Content-Type'] = force_type
        elif self.is_valid() and self._format:
            # Set MIME type for valid formats
            headers['Content-Type'] = FORMAT_TYPES[self._format]

        if F_GZIP in FORMAT_TYPES:
            if force_encoding:
                headers['Content-Encoding'] = force_encoding
            elif F_GZIP in self._headers.get('Accept-Encoding', ''):
                headers['Content-Encoding'] = F_GZIP

        return headers

    def get_request_headers(self, headers) -> dict:
        """
        Obtains and returns a dictionary with Request object headers.

        This method adds the headers of the original request and
        makes them available to the API object.

        :returns: A header dict
        """

        headers_ = {item[0]: item[1] for item in headers.items()}
        return headers_


class API:
    """API object"""

    def __init__(self, config):
        """
        constructor

        :param config: configuration dict

        :returns: `pygeoapi.API` instance
        """

        self.config = config
        self.config['server']['url'] = self.config['server']['url'].rstrip('/')

        CHARSET[0] = config['server'].get('encoding', 'utf-8')
        if config['server'].get('gzip') is True:
            FORMAT_TYPES[F_GZIP] = 'application/gzip'
            FORMAT_TYPES.move_to_end(F_JSON)

        # Process language settings (first locale is default!)
        self.locales = l10n.get_locales(config)
        self.default_locale = self.locales[0]

        if 'templates' not in self.config['server']:
            self.config['server']['templates'] = {'path': TEMPLATES}

        if 'pretty_print' not in self.config['server']:
            self.config['server']['pretty_print'] = False

        self.pretty_print = self.config['server']['pretty_print']

        setup_logger(self.config['logging'])

        # TODO: add as decorator
        if 'manager' in self.config['server']:
            manager_def = self.config['server']['manager']
        else:
            LOGGER.info('No process manager defined; starting dummy manager')
            manager_def = {
                'name': 'Dummy',
                'connection': None,
                'output_dir': None
            }

        LOGGER.debug('Loading process manager {}'.format(manager_def['name']))
        self.manager = load_plugin('process_manager', manager_def)
        LOGGER.info('Process manager plugin loaded')

        # TODO: add movingfeatures datasource as database
        # self.datasource = self.config['datasource']
        # self.pd = ProcessMobilityData(self.datasource)
    @gzip
    @pre_process
    @jsonldify
    def landing_page(self,
                     request: Union[APIRequest, Any]) -> Tuple[dict, int, str]:
        """
        Provide API landing page

        :param request: A request object

        :returns: tuple of headers, status code, content
        """

        if not request.is_valid():
            return self.get_format_exception(request)

        fcm = {
            'title': l10n.translate(
                self.config['metadata']['identification']['title'],
                request.locale),
            'description':
                l10n.translate(
                    self.config['metadata']['identification']['description'],
                    request.locale),
            'links': []
        }

        LOGGER.debug('Creating links')
        # TODO: put title text in config or translatable files?
        fcm['links'] = [{            
            'href': '{}/{}'.format(self.config['server']['url'], 'api'),
            'rel': 'alternate',
            'type': 'application/geo+json',
            'hreflang': 'en',
            'title': 'API definition',
            'length': 0
        }, {            
            'href': '{}/{}'.format(self.config['server']['url'], 'conformance'),
            'rel': 'alternate',
            'type': 'application/geo+json',
            'hreflang': 'en',
            'title': 'conformance statements',
            'length': 0
        }, {            
            'href': '{}'.format(self.get_collections_url()),
            'rel': 'alternate',
            'type': 'application/geo+json',
            'hreflang': 'en',
            'title': 'feature collections',
            'length': 0
        }
        ]

        headers = request.get_response_headers()
        if request.format == F_HTML:  # render

            fcm['processes'] = False
            fcm['stac'] = False

            content = render_j2_template(self.config, 'landing_page.html', fcm,
                                         request.locale)
            return headers, 200, content

        if request.format == F_JSONLD:
            return headers, 200, to_json(self.fcmld, self.pretty_print)
        return headers, 200, to_json(fcm, self.pretty_print)

    @gzip
    @pre_process
    def openapi(self, request: Union[APIRequest, Any],
                openapi) -> Tuple[dict, int, str]:
        """
        Provide OpenAPI document

        :param request: A request object
        :param openapi: dict of OpenAPI definition

        :returns: tuple of headers, status code, content
        """

        if not request.is_valid():
            return self.get_format_exception(request)

        headers = request.get_response_headers()

        if request.format == F_HTML:
            template = 'openapi/swagger.html'
            if request._args.get('ui') == 'redoc':
                template = 'openapi/redoc.html'

            path = '/'.join([self.config['server']['url'].rstrip('/'),
                            'openapi'])
            data = {
                'openapi-document-path': path
            }
            content = render_j2_template(self.config, template, data,
                                         request.locale)
            return headers, 200, content

        headers['Content-Type'] = 'application/vnd.oai.openapi+json;version=3.0'  # noqa

        if isinstance(openapi, dict):
            return headers, 200, to_json(openapi, self.pretty_print)
        else:
            return headers, 200, openapi

    @gzip
    @pre_process
    def conformance(self,
                    request: Union[APIRequest, Any]) -> Tuple[dict, int, str]:
        """
        Provide conformance definition

        :param request: A request object

        :returns: tuple of headers, status code, content
        """

        if not request.is_valid():
            return self.get_format_exception(request)

        conformance_list = CONFORMANCE['movingfeatures']

        conformance = {
            'conformsTo': list(set(conformance_list))
        }

        headers = request.get_response_headers()
        if request.format == F_HTML:  # render
            content = render_j2_template(self.config, 'conformance.html',
                                        conformance, request.locale)
            return headers, 200, content

        return headers, 200, to_json(conformance, self.pretty_print)

    @gzip
    @pre_process
    @jsonldify
    def describe_collections(self, request: Union[APIRequest, Any]) -> Tuple[dict, int, str]: 
        """
        Queries collection

        :param request: A request object

        :returns: tuple of headers, status code, content
        """
        if not request.is_valid():
            return self.get_format_exception(request)
        headers = request.get_response_headers()

        pd = ProcessMobilityData()
        fcm = {
            'collections': [],
            'links': []
        }

        try:              
            pd.connect()   
            rows = pd.getCollections() 
        except (Exception, psycopg2.Error) as error:
            msg = str(error)
            return self.get_exception(
            400, headers, request.format, 'ConnectingError', msg) 

        collections = []
        for row in rows:
            collection_id = row[0]
            collection = row[1]
            collection['itemType'] = 'movingfeature'
            collection['id'] = collection_id

            crs = None
            trs = None
            if 'crs' in collection:
                crs = collection.pop('crs', None)
            if 'trs' in collection:
                trs = collection.pop('trs', None)

            bbox = []            
            extend_stbox = row[3]
            if extend_stbox is not None :
                bbox.append(extend_stbox.xmin)
                bbox.append(extend_stbox.ymin)
                if extend_stbox.zmin is not None:
                    bbox.append(extend_stbox.zmin)
                bbox.append(extend_stbox.xmax)
                bbox.append(extend_stbox.ymax)
                if extend_stbox.zmax is not None:
                    bbox.append(extend_stbox.zmax)

                if crs is None:
                    if extend_stbox.srid == False or row[2].srid == 4326:
                        if extend_stbox.zmin is not None:
                            crs = 'http://www.opengis.net/def/crs/OGC/0/CRS84h'
                        else:                    
                            crs = 'http://www.opengis.net/def/crs/OGC/1.3/CRS84'
            if crs is None:
                crs = 'http://www.opengis.net/def/crs/OGC/1.3/CRS84'  
            if trs is None:
                trs = 'http://www.opengis.net/def/uom/ISO-8601/0/Gregorian'

            time = []
            lifespan = row[2]
            if lifespan is not None :
                time.append(lifespan._lower.strftime("%Y/%m/%dT%H:%M:%SZ"))
                time.append(lifespan._upper.strftime("%Y/%m/%dT%H:%M:%SZ"))

            collection['extent'] = {
                'spatial': {
                    'bbox': bbox,
                    'crs': crs
                },
                'temporal': {
                    'interval': time,
                    'trs': trs
                }
            }

            collection['links'] = []
            collection['links'].append({
                'href': '{}/{}'.format(
                    self.get_collections_url(), collection_id),
                'rel': request.get_linkrel(F_JSON),
                'type': FORMAT_TYPES[F_JSON]
            })
            collections.append(collection)
        fcm['collections'] = collections        
        fcm['links'].append({
            'href': '{}'.format(
                self.get_collections_url()),
            'rel': request.get_linkrel(F_JSON),
            'type': FORMAT_TYPES[F_JSON]
        })
        return headers, 200, to_json(fcm, self.pretty_print)

    @gzip
    @pre_process
    @jsonldify
    def manage_collection(self, request: Union[APIRequest, Any], 
            action, dataset=None) -> Tuple[dict, int, str]:        
        """
        Adds a collection

        :param request: A request object
        :param dataset: dataset name

        :returns: tuple of headers, status code, content
        """

        headers = request.get_response_headers(SYSTEM_LOCALE)
        pd = ProcessMobilityData()
        collection_id = str(dataset)
        if action in ['create', 'update']:
            data = request.data
            if not data:
                # TODO not all processes require input, e.g. time-dependent or
                #      random value generators
                msg = 'missing request data'
                return self.get_exception(
                    400, headers, request.format, 'MissingParameterValue', msg)

            try:
                # Parse bytes data, if applicable
                data = data.decode()
                LOGGER.debug(data)
            except (UnicodeDecodeError, AttributeError):
                pass

            try:
                data = json.loads(data)
            except (json.decoder.JSONDecodeError, TypeError) as err:
                # Input does not appear to be valid JSON
                LOGGER.error(err)
                msg = 'invalid request data'
                return self.get_exception(
                    400, headers, request.format, 'InvalidParameterValue', msg)
            
        if action == 'create':   
            try:
                pd.connect()              
                collection_id = pd.postCollection(data)            
            except (Exception, psycopg2.Error) as error:
                msg = str(error)
                return self.get_exception(
                400, headers, request.format, 'ConnectingError', msg)    
            finally:
                pd.disconnect()
                
            url = '{}/{}'.format(self.get_collections_url(), collection_id)

            headers['Location'] = url  
            return headers, 201, ''
        
        if action == 'update':
            LOGGER.debug('Updating item')   
            try:
                pd.connect()   
                pd.putCollection(collection_id, data)
            except (Exception, psycopg2.Error) as error:
                msg = str(error)
                return self.get_exception(
                400, headers, request.format, 'ConnectingError', msg)    
            finally:
                pd.disconnect()
            
            return headers, 204, ''
        
        if action == 'delete':
            LOGGER.debug('Deleting item')
            try:
                pd.connect()   
                pd.deleteCollection("AND collection_id ='{0}'".format(collection_id))
                
            except (Exception, psycopg2.Error) as error:
                msg = str(error)
                return self.get_exception(
                400, headers, request.format, 'ConnectingError', msg)    
            finally:
                pd.disconnect()
            
            return headers, 204, ''

    @gzip
    @pre_process
    @jsonldify
    def get_collection(self, request: Union[APIRequest, Any], 
            dataset=None) -> Tuple[dict, int, str]:        
        """
        Queries collection

        :param request: A request object
        :param dataset: dataset name

        :returns: tuple of headers, status code, content
        """
        pd = ProcessMobilityData()
        collection_id = str(dataset)
        if not request.is_valid():
            return self.get_format_exception(request)
        headers = request.get_response_headers()

        try:           
            pd.connect()   
            rows = pd.getCollection(collection_id)
            if len(rows) > 0:
                row = rows[0]        
            else:                    
                msg = 'Collection not found'
                LOGGER.error(msg)
                return self.get_exception(
                    404, headers, request.format, 'NotFound', msg)      
        except (Exception, psycopg2.Error) as error:
            msg = str(error)
            return self.get_exception(
            400, headers, request.format, 'ConnectingError', msg)
    

        collection = {}
        if row != None:       
            collection_id = row[0]
            collection = row[1]
            collection['itemType'] = 'movingfeature'    
            collection['id'] = collection_id
            
            crs = None
            trs = None
            if 'crs' in collection:
                crs = collection.pop('crs', None)
            if 'trs' in collection:
                trs = collection.pop('trs', None)

            bbox = []            
            extend_stbox = row[3]
            if extend_stbox is not None :
                bbox.append(extend_stbox.xmin)
                bbox.append(extend_stbox.ymin)
                if extend_stbox.zmin is not None:
                    bbox.append(extend_stbox.zmin)
                bbox.append(extend_stbox.xmax)
                bbox.append(extend_stbox.ymax)
                if extend_stbox.zmax is not None:
                    bbox.append(extend_stbox.zmax)

                if crs is None:
                    if extend_stbox.srid == False or row[2].srid == 4326:
                        if extend_stbox.zmin is not None:
                            crs = 'http://www.opengis.net/def/crs/OGC/0/CRS84h'
                        else:                    
                            crs = 'http://www.opengis.net/def/crs/OGC/1.3/CRS84'
                
            if crs is None:
                crs = 'http://www.opengis.net/def/crs/OGC/1.3/CRS84'
            if trs is None:
                trs = 'http://www.opengis.net/def/uom/ISO-8601/0/Gregorian'

            time = []
            lifespan = row[2]
            if lifespan is not None :
                time.append(lifespan._lower.strftime("%Y/%m/%dT%H:%M:%SZ"))
                time.append(lifespan._upper.strftime("%Y/%m/%dT%H:%M:%SZ"))

            collection['extent'] = {
                'spatial': {
                    'bbox': bbox,
                    'crs': crs
                },
                'temporal': {
                    'interval': time,
                    'trs': trs
                }
            }

            collection['links'] = []
            collection['links'].append({
                'href': '{}/{}'.format(
                    self.get_collections_url(), collection_id),
                'rel': request.get_linkrel(F_JSON),
                'type': FORMAT_TYPES[F_JSON]
            })
        return headers, 200, to_json(collection, self.pretty_print)

    @gzip
    @pre_process
    def get_collection_items(
            self, request: Union[APIRequest, Any],
            dataset) -> Tuple[dict, int, str]:
        """
        Queries collection

        :param request: A request object
        :param dataset: dataset name

        :returns: tuple of headers, status code, content
        """

        # Set Content-Language to system locale until provider locale
        # has been determined
        
        if not request.is_valid():
            return self.get_format_exception(request)
        headers = request.get_response_headers(SYSTEM_LOCALE)

        excuted, collections = getListOfCollectionsId()
        collection_id = dataset
        if excuted == False:
            msg = str(collections)
            return self.get_exception(
            400, headers, request.format, 'ConnectingError', msg)   

        if collection_id not in collections:
            msg = 'Collection not found'
            LOGGER.error(msg)
            return self.get_exception(
                404, headers, request.format, 'NotFound', msg)                

        LOGGER.debug('Processing query parameters')

        LOGGER.debug('Processing offset parameter')
        try:
            offset = int(request.params.get('offset'))
            if offset < 0:
                msg = 'offset value should be positive or zero'
                return self.get_exception(
                    400, headers, request.format, 'InvalidParameterValue', msg)
        except TypeError as err:
            LOGGER.warning(err)
            offset = 0
        except ValueError:
            msg = 'offset value should be an integer'
            return self.get_exception(
                400, headers, request.format, 'InvalidParameterValue', msg)

        LOGGER.debug('Processing limit parameter')
        try:
            limit = int(request.params.get('limit'))
            # TODO: We should do more validation, against the min and max
            #       allowed by the server configuration
            if limit <= 0:
                msg = 'limit value should be strictly positive'
                return self.get_exception(
                    400, headers, request.format, 'InvalidParameterValue', msg)
            if limit > 10000:
                msg = 'limit value should be less than or equal to 10000'
                return self.get_exception(
                    400, headers, request.format, 'InvalidParameterValue', msg)
        except TypeError as err:
            LOGGER.warning(err)
            limit = int(self.config['server']['limit'])
        except ValueError:
            msg = 'limit value should be an integer'
            return self.get_exception(
                400, headers, request.format, 'InvalidParameterValue', msg)

        LOGGER.debug('Processing bbox parameter')

        bbox = request.params.get('bbox')

        if bbox is None:
            bbox = []
        else:
            try:
                bbox = validate_bbox(bbox)
            except ValueError as err:
                msg = str(err)
                return self.get_exception(
                    400, headers, request.format, 'InvalidParameterValue', msg)

        LOGGER.debug('Processing datetime parameter')
        datetime_ = request.params.get('datetime')
        try:
            datetime_ = validate_datetime(datetime_)
        except ValueError as err:
            msg = str(err)
            return self.get_exception(
                400, headers, request.format, 'InvalidParameterValue', msg)

        subTrajectory = request.params.get('subTrajectory')
        if subTrajectory is None:
            subTrajectory = False

        LOGGER.debug('Querying provider')
        LOGGER.debug('offset: {}'.format(offset))
        LOGGER.debug('limit: {}'.format(limit))
        LOGGER.debug('bbox: {}'.format(bbox))
        LOGGER.debug('datetime: {}'.format(datetime_))

        pd = ProcessMobilityData()
        content = {
            "type": "FeatureCollection",
            "features": [],
            "crs":{},
            "trs":{},
            "links":[]
        }

        try:     
            pd.connect()    
            rows, numberMatched, numberReturned = pd.getFeatures(collection_id=collection_id,bbox=bbox,datetime=datetime_,limit=limit,offset=offset,subTrajectory=subTrajectory)
        except (Exception, psycopg2.Error) as error:
            msg = str(error)
            return self.get_exception(
            400, headers, request.format, 'ConnectingError', msg) 

        mfeatures = []
        crs = None
        trs = None

        split_mfeature = {}
        for i in range(len(rows)): 
            mfeature_id = str(rows[i][1])
            if mfeature_id not in split_mfeature:
                split_mfeature[mfeature_id] = []
            split_mfeature[mfeature_id].append(i)  

        pymeos_initialize()   
        for key, mfeature_row_index in split_mfeature.items(): 
            row = rows[mfeature_row_index[0]]
            
            mfeature_id = row[1]
            mfeature = row[3]
            mfeature['id'] = mfeature_id
            mfeature['type'] = 'Feature'

            if 'crs' in mfeature and crs == None:
                crs = mfeature['crs']
            if 'trs' in mfeature and trs == None:
                trs = mfeature['trs']

            if row[2] != None:
                mfeature['geometry'] = json.loads(row[2]) 
            else:
                mfeature['geometry'] = None

            if 'properties' not in mfeature:
                mfeature['properties'] = None

            if subTrajectory == True or subTrajectory == "true":
                prisms = []
                for row_index in mfeature_row_index:
                    row_tgeometory = rows[int(row_index)]                
                    if row_tgeometory[7] is not None:
                        mfeature_check = row_tgeometory[1]
                        if mfeature_check == mfeature_id:
                            temporalGeometry = json.loads(Temporal.as_mfjson(TGeomPointSeq(str(row_tgeometory[7]).replace("'","")),False))
                            if 'crs' in temporalGeometry and crs == None:
                                crs = temporalGeometry['crs']
                            if 'trs' in temporalGeometry and trs == None:
                                trs = temporalGeometry['trs']
                            temporalGeometry = pd.convertTemporalGeometryToOldVersion(temporalGeometry)
                            temporalGeometry['id'] = row_tgeometory[6]
                            prisms.append(temporalGeometry)
                mfeature['temporalGeometry'] = prisms
            bbox = []
            extend_stbox = row[5]
            if extend_stbox is not None :
                bbox.append(extend_stbox.xmin)
                bbox.append(extend_stbox.ymin)
                if extend_stbox.zmin is not None:
                    bbox.append(extend_stbox.zmin)
                bbox.append(extend_stbox.xmax)
                bbox.append(extend_stbox.ymax)
                if extend_stbox.zmax is not None:
                    bbox.append(extend_stbox.zmax)
            mfeature['bbox'] = bbox

            time = []
            lifespan = row[4]
            if lifespan is not None :
                time.append(lifespan._lower.strftime("%Y/%m/%dT%H:%M:%SZ"))
                time.append(lifespan._upper.strftime("%Y/%m/%dT%H:%M:%SZ"))
            mfeature['time'] = time

            if 'crs' not in mfeature:
                mfeature['crs'] = {
                    "type":"Name",
                    "properties":"urn:ogc:def:crs:OGC:1.3:CRS84"
                }
            if 'trs' not in mfeature:
                mfeature['trs'] = {
                    "type":"Name",
                    "properties":"urn:ogc:data:time:iso8601"
                }
            mfeatures.append(mfeature)

        content['features'] = mfeatures
        if crs != None:
            content['crs'] = crs
        else:
            content['crs'] = {
                "type":"Name",
                "properties":"urn:ogc:def:crs:OGC:1.3:CRS84"
            }

        if trs != None:
            content['trs'] = trs
        else:
            content['trs'] = {
                "type":"Name",
                "properties":"urn:ogc:data:time:iso8601"
            }


        # TODO: translate titles
        uri = '{}/{}/items'.format(self.get_collections_url(), collection_id)       

        serialized_query_params = ''
        for k, v in request.params.items():
            if k not in ('f', 'offset'):
                serialized_query_params += '&'
                serialized_query_params += urllib.parse.quote(k, safe='')
                serialized_query_params += '='
                serialized_query_params += urllib.parse.quote(str(v), safe=',')

        content['links'] = [{
                'href': '{}?offset={}{}'.format(uri, offset, serialized_query_params),
                'rel': request.get_linkrel(F_JSON),
                'type': FORMAT_TYPES[F_JSON]
            }]
        
        if len(content['features']) == limit:
            next_ = offset + limit
            content['links'].append(
                {
                    'href': '{}?offset={}{}'.format(uri, next_, serialized_query_params),
                    'type': 'application/geo+json',
                    'rel': 'next'
                })

        content['timeStamp'] = datetime.utcnow().strftime(
            '%Y-%m-%dT%H:%M:%S.%fZ')

        content['numberMatched'] = numberMatched
        content['numberReturned'] = numberReturned
        return headers, 200, to_json(content, self.pretty_print)

    @gzip
    @pre_process
    def manage_collection_item(
            self, request: Union[APIRequest, Any],
            action, dataset, identifier=None) -> Tuple[dict, int, str]:
        """
        Adds an item to a collection

        :param request: A request object
        :param dataset: dataset name

        :returns: tuple of headers, status code, content
        """

        if not request.is_valid(PLUGINS['formatter'].keys()):
            return self.get_format_exception(request)

        # Set Content-Language to system locale until provider locale
        # has been determined
        headers = request.get_response_headers(SYSTEM_LOCALE)

        pd = ProcessMobilityData()
        excuted, collections = getListOfCollectionsId()

        if excuted == False:
            msg = str(collections)
            return self.get_exception(
            400, headers, request.format, 'ConnectingError', msg)   

        if dataset not in collections:
            msg = 'Collection not found'
            LOGGER.error(msg)
            return self.get_exception(
                404, headers, request.format, 'NotFound', msg)

        collectionId = dataset
        mfeature_id = identifier
        if action == 'create':
            if not request.data:
                msg = 'No data found'
                LOGGER.error(msg)
                return self.get_exception(
                    400, headers, request.format, 'InvalidParameterValue', msg)
            
            data = request.data
            try:
                # Parse bytes data, if applicable
                data = data.decode()
                LOGGER.debug(data)
            except (UnicodeDecodeError, AttributeError):
                pass

            try:
                data = json.loads(data)
            except (json.decoder.JSONDecodeError, TypeError) as err:
                # Input does not appear to be valid JSON
                LOGGER.error(err)
                msg = 'invalid request data'
                return self.get_exception(
                    400, headers, request.format, 'InvalidParameterValue', msg)
            
            if checkRequiredFieldFeature(data) == False:
                # TODO not all processes require input
                msg = 'The required tag (e.g., type,temporalgeometry) is missing from the request data.'
                return self.get_exception(
                    501, headers, request.format, 'MissingParameterValue', msg)

            LOGGER.debug('Creating item')
            try:
                pd.connect()       
                if data['type'] == 'FeatureCollection':
                    for feature in data['features']:
                        mfeature_id = pd.postMovingFeature(collectionId, feature)
                else:
                    # for _ in range(10000):
                        mfeature_id = pd.postMovingFeature(collectionId, data)
            except (Exception, psycopg2.Error) as error:
                msg = str(error)
                return self.get_exception(
                400, headers, request.format, 'ConnectingError', msg)    
            finally:
                pd.disconnect() 

            headers['Location'] = '{}/{}/items/{}'.format(
                self.get_collections_url(), dataset, mfeature_id)

            return headers, 201, ''

        if action == 'delete':
            LOGGER.debug('Deleting item')

            try:
                pd.connect()   
                pd.deleteMovingFeature("AND mfeature_id ='{0}'".format(mfeature_id))
                
            except (Exception, psycopg2.Error) as error:
                msg = str(error)
                return self.get_exception(
                400, headers, request.format, 'ConnectingError', msg)    
            finally:
                pd.disconnect()
            
            return headers, 204, ''

    @gzip
    @pre_process
    def get_collection_item(self, request: Union[APIRequest, Any],
                            dataset, identifier) -> Tuple[dict, int, str]:
        """
        Get a single collection item

        :param request: A request object
        :param dataset: dataset name
        :param identifier: item identifier

        :returns: tuple of headers, status code, content
        """

        pd = ProcessMobilityData()
        collection_id = str(dataset)
        mfeature_id = str(identifier)
        if not request.is_valid():
            return self.get_format_exception(request)
        headers = request.get_response_headers()

        try:           
            pd.connect()   
            rows = pd.getFeature(collection_id, mfeature_id)
            if len(rows) > 0:
                row = rows[0]        
            else:                    
                msg = 'Feature not found'
                LOGGER.error(msg)
                return self.get_exception(
                    404, headers, request.format, 'NotFound', msg)      
        except (Exception, psycopg2.Error) as error:
            msg = str(error)
            return self.get_exception(
            400, headers, request.format, 'ConnectingError', msg) 

        mfeature = {}
        if row != None:        
            mfeature_id = row[1]
            mfeature = row[3]
            mfeature['id'] = mfeature_id
            mfeature['type'] = 'Feature'
            
            if row[2] != None:
                mfeature['geometry'] = json.loads(row[2])
            
            bbox = []
            extend_stbox = row[5]
            if extend_stbox is not None :
                bbox.append(extend_stbox.xmin)
                bbox.append(extend_stbox.ymin)
                if extend_stbox.zmin is not None:
                    bbox.append(extend_stbox.zmin)
                bbox.append(extend_stbox.xmax)
                bbox.append(extend_stbox.ymax)
                if extend_stbox.zmax is not None:
                    bbox.append(extend_stbox.zmax)
            mfeature['bbox'] = bbox

            time = []
            lifespan = row[4]
            if lifespan is not None :
                time.append(lifespan._lower.strftime("%Y/%m/%dT%H:%M:%SZ"))
                time.append(lifespan._upper.strftime("%Y/%m/%dT%H:%M:%SZ"))
            mfeature['time'] = time

            if 'crs' not in mfeature:
                mfeature['crs'] = {
                    "type":"Name",
                    "properties":"urn:ogc:def:crs:OGC:1.3:CRS84"
                }
            if 'trs' not in mfeature:
                mfeature['trs'] = {
                    "type":"Name",
                    "properties":"urn:ogc:data:time:iso8601"
                }
            mfeature['links'] = []
            mfeature['links'].append({
                'href': '{}/{}/items/{}'.format(
                    self.get_collections_url(), collection_id, mfeature_id),
                'rel': request.get_linkrel(F_JSON),
                'type': FORMAT_TYPES[F_JSON]
            })
        return headers, 200, to_json(mfeature, self.pretty_print)

    @gzip
    @pre_process
    def get_collection_items_tGeometry(self, request: Union[APIRequest, Any],
                            dataset, identifier) -> Tuple[dict, int, str]:
        """
        Get temporal Geometry of collection item

        :param request: A request object
        :param dataset: dataset name
        :param identifier: item identifier

        :returns: tuple of headers, status code, content
        """

        if not request.is_valid():
            return self.get_format_exception(request)
        headers = request.get_response_headers(SYSTEM_LOCALE)

        excuted, featureList = getListOfFeaturesId()
        if excuted == False:
            msg = str(featureList)
            return self.get_exception(
            400, headers, request.format, 'ConnectingError', msg)   

        if [dataset, identifier] not in featureList:
            msg = 'Feature not found'
            LOGGER.error(msg)
            return self.get_exception(
                404, headers, request.format, 'NotFound', msg)           

        collection_id = dataset
        mfeature_id = identifier
        LOGGER.debug('Processing query parameters')

        LOGGER.debug('Processing offset parameter')
        try:
            offset = int(request.params.get('offset'))
            if offset < 0:
                msg = 'offset value should be positive or zero'
                return self.get_exception(
                    400, headers, request.format, 'InvalidParameterValue', msg)
        except TypeError as err:
            LOGGER.warning(err)
            offset = 0
        except ValueError:
            msg = 'offset value should be an integer'
            return self.get_exception(
                400, headers, request.format, 'InvalidParameterValue', msg)

        LOGGER.debug('Processing limit parameter')
        try:
            limit = int(request.params.get('limit'))
            # TODO: We should do more validation, against the min and max
            #       allowed by the server configuration
            if limit <= 0:
                msg = 'limit value should be strictly positive'
                return self.get_exception(
                    400, headers, request.format, 'InvalidParameterValue', msg)
            if limit > 10000:
                msg = 'limit value should be less than or equal to 10000'
                return self.get_exception(
                    400, headers, request.format, 'InvalidParameterValue', msg)
        except TypeError as err:
            LOGGER.warning(err)
            limit = int(self.config['server']['limit'])
        except ValueError:
            msg = 'limit value should be an integer'
            return self.get_exception(
                400, headers, request.format, 'InvalidParameterValue', msg)

        LOGGER.debug('Processing bbox parameter')

        bbox = request.params.get('bbox')

        if bbox is None:
            bbox = []
        else:
            try:
                bbox = validate_bbox(bbox)
            except ValueError as err:
                msg = str(err)
                return self.get_exception(
                    400, headers, request.format, 'InvalidParameterValue', msg)

        leaf_ = request.params.get('leaf')       
        LOGGER.debug('Processing leaf parameter')
        try:
            leaf_ = validate_leaf(leaf_)
        except ValueError as err:
            msg = str(err)
            return self.get_exception(
                400, headers, request.format, 'InvalidParameterValue', msg)

        subTrajectory = request.params.get('subTrajectory')
        if subTrajectory is None:
            subTrajectory = False        

        if (leaf_ != '' and leaf_ is not None) and (subTrajectory == True or subTrajectory == 'true'):
            msg = 'Cannot use both parameter `subTrajectory` and `leaf` at the same time'
            return self.get_exception(
                400, headers, request.format, 'InvalidParameterValue', msg)

        LOGGER.debug('Processing datetime parameter')
        datetime_ = request.params.get('datetime')
        try:
            datetime_ = validate_datetime(datetime_)
        except ValueError as err:
            msg = str(err)
            return self.get_exception(
                400, headers, request.format, 'InvalidParameterValue', msg)

        LOGGER.debug('Querying provider')
        LOGGER.debug('offset: {}'.format(offset))
        LOGGER.debug('limit: {}'.format(limit))
        LOGGER.debug('bbox: {}'.format(bbox))
        LOGGER.debug('leaf: {}'.format(leaf_))
        LOGGER.debug('datetime: {}'.format(datetime_))
        
        pd = ProcessMobilityData()
        content = {
            "type": "MovingGeometryCollection",
            "prisms": [],
            "crs":{},
            "trs":{},
            "links":[],
        }

        crs = None
        trs = None
        try:             
            pd.connect()
            rows, numberMatched, numberReturned = pd.getTemporalGeometries(collection_id=collection_id,mfeature_id=mfeature_id,bbox=bbox,leaf=leaf_,datetime=datetime_,limit=limit,offset=offset,subTrajectory=subTrajectory)
            pymeos_initialize()
            prisms = []
            for row in rows:   
                temporalGeometry = json.loads(Temporal.as_mfjson(TGeomPointSeq(str(row[3]).replace("'","")),False))
                if 'crs' in temporalGeometry and crs == None:
                    crs = temporalGeometry['crs']
                if 'trs' in temporalGeometry and trs == None:
                    trs = temporalGeometry['trs']
                temporalGeometry = pd.convertTemporalGeometryToOldVersion(temporalGeometry)
                temporalGeometry['id'] = row[2]

                if (leaf_ != '' and leaf_ is not None) or (subTrajectory == True or subTrajectory == 'true'):
                    if row[4] is not None:
                        temporalGeometry_filter = json.loads(Temporal.as_mfjson(TGeomPointSeq(str(row[4]).replace("'","")),False))
                        temporalGeometry['datetimes'] = temporalGeometry_filter['datetimes']
                        temporalGeometry['coordinates'] = temporalGeometry_filter['coordinates']
                    else:
                        temporalGeometry['datetimes'] = []
                        temporalGeometry['coordinates'] = []
                prisms.append(temporalGeometry)
            content["prisms"] = prisms
        except (Exception, psycopg2.Error) as error:
            msg = str(error)
            return self.get_exception(
            400, headers, request.format, 'ConnectingError', msg) 

        if crs != None:
            content['crs'] = crs
        else:
            content['crs'] = {
                "type":"Name",
                "properties":"urn:ogc:def:crs:OGC:1.3:CRS84"
            }

        if trs != None:
            content['trs'] = trs
        else:
            content['trs'] = {
                "type":"Name",
                "properties":"urn:ogc:data:time:iso8601"
            }

        # TODO: translate titles
        uri = '{}/{}/items/{}/tGeometries'.format(self.get_collections_url(), collection_id, mfeature_id)        

        serialized_query_params = ''
        for k, v in request.params.items():
            if k not in ('f', 'offset'):
                serialized_query_params += '&'
                serialized_query_params += urllib.parse.quote(k, safe='')
                serialized_query_params += '='
                serialized_query_params += urllib.parse.quote(str(v), safe=',')

        content['links'] = [{
                'href': '{}?offset={}{}'.format(uri, offset, serialized_query_params),
                'rel': request.get_linkrel(F_JSON),
                'type': FORMAT_TYPES[F_JSON]
            }]
        
        if len(content['prisms']) == limit:
            next_ = offset + limit
            content['links'].append(
                {
                    'href': '{}?offset={}{}'.format(uri, next_, serialized_query_params),
                    'type': 'application/geo+json',
                    'rel': 'next'
                })

        content['timeStamp'] = datetime.utcnow().strftime(
            '%Y-%m-%dT%H:%M:%S.%fZ')

        content['numberMatched'] = numberMatched
        content['numberReturned'] = numberReturned
        return headers, 200, to_json(content, self.pretty_print)

    @gzip
    @pre_process
    def manage_collection_item_tGeometry(
            self, request: Union[APIRequest, Any],
            action, dataset, identifier, tGeometry=None) -> Tuple[dict, int, str]:
        """
        Adds Temporal Geometry item to a moving feature

        :param request: A request object
        :param dataset: dataset name
        :param identifier: moving feature's id 
        :param tGeometry: Temporal Geometry's id 

        :returns: tuple of headers, status code, content
        """

        if not request.is_valid(PLUGINS['formatter'].keys()):
            return self.get_format_exception(request)

        # Set Content-Language to system locale until provider locale
        # has been determined
        headers = request.get_response_headers(SYSTEM_LOCALE)

        pd = ProcessMobilityData()
        excuted, featureList = getListOfFeaturesId()

        if excuted == False:
            msg = str(featureList)
            return self.get_exception(
            400, headers, request.format, 'ConnectingError', msg)   

        if [dataset, identifier] not in featureList:
            msg = 'Feature not found'
            LOGGER.error(msg)
            return self.get_exception(
                404, headers, request.format, 'NotFound', msg)

        collectionId = dataset
        mfeature_id = identifier
        tGeometry_id = tGeometry
        if action == 'create':
            if not request.data:
                msg = 'No data found'
                LOGGER.error(msg)
                return self.get_exception(
                    400, headers, request.format, 'InvalidParameterValue', msg)
            
            data = request.data
            try:
                # Parse bytes data, if applicable
                data = data.decode()
                LOGGER.debug(data)
            except (UnicodeDecodeError, AttributeError):
                pass

            try:
                data = json.loads(data)
            except (json.decoder.JSONDecodeError, TypeError) as err:
                # Input does not appear to be valid JSON
                LOGGER.error(err)
                msg = 'invalid request data'
                return self.get_exception(
                    400, headers, request.format, 'InvalidParameterValue', msg)
            
            if checkRequiredFieldTemporalGeometries(data) == False:
                # TODO not all processes require input
                msg = 'The required tag (e.g., type,temporalgeometry) is missing from the request data.'
                return self.get_exception(
                    501, headers, request.format, 'MissingParameterValue', msg)
            
            LOGGER.debug('Creating item')
            try:
                pd.connect()       
                if data['type'] == 'MovingGeometryCollection':
                    for tGeometry in data['prisms']:
                        tGeometry_id = pd.postTemporalGeometry(collectionId, mfeature_id, tGeometry)
                    
                else:
                    tGeometry_id = pd.postTemporalGeometry(collectionId, mfeature_id, data)
            except (Exception, psycopg2.Error) as error:
                msg = str(error)
                return self.get_exception(
                400, headers, request.format, 'ConnectingError', msg)    
            finally:
                pd.disconnect() 

            headers['Location'] = '{}/{}/items/{}/tGeometries/{}'.format(
                self.get_collections_url(), dataset, mfeature_id, tGeometry_id)

            return headers, 201, ''

        if action == 'delete':
            LOGGER.debug('Deleting item')

            try:
                pd.connect()   
                pd.deleteTemporalGeometry("AND tgeometry_id ='{0}'".format(tGeometry_id))
                
            except (Exception, psycopg2.Error) as error:
                msg = str(error)
                return self.get_exception(
                400, headers, request.format, 'ConnectingError', msg)    
            finally:
                pd.disconnect()
            
            return headers, 204, ''

    @gzip
    @pre_process
    def get_collection_items_tProperty(self, request: Union[APIRequest, Any],
                            dataset, identifier) -> Tuple[dict, int, str]:
        """
        Get temporal Properties of collection item

        :param request: A request object
        :param dataset: dataset name
        :param identifier: item identifier

        :returns: tuple of headers, status code, content
        """

        if not request.is_valid():
            return self.get_format_exception(request)
        headers = request.get_response_headers(SYSTEM_LOCALE)

        excuted, featureList = getListOfFeaturesId()
        if excuted == False:
            msg = str(featureList)
            return self.get_exception(
            400, headers, request.format, 'ConnectingError', msg)   

        if [dataset, identifier] not in featureList:
            msg = 'Feature not found'
            LOGGER.error(msg)
            return self.get_exception(
                404, headers, request.format, 'NotFound', msg)           

        collection_id = dataset
        mfeature_id = identifier
        LOGGER.debug('Processing query parameters')

        LOGGER.debug('Processing offset parameter')
        try:
            offset = int(request.params.get('offset'))
            if offset < 0:
                msg = 'offset value should be positive or zero'
                return self.get_exception(
                    400, headers, request.format, 'InvalidParameterValue', msg)
        except TypeError as err:
            LOGGER.warning(err)
            offset = 0
        except ValueError:
            msg = 'offset value should be an integer'
            return self.get_exception(
                400, headers, request.format, 'InvalidParameterValue', msg)

        LOGGER.debug('Processing limit parameter')
        try:
            limit = int(request.params.get('limit'))
            # TODO: We should do more validation, against the min and max
            #       allowed by the server configuration
            if limit <= 0:
                msg = 'limit value should be strictly positive'
                return self.get_exception(
                    400, headers, request.format, 'InvalidParameterValue', msg)
            if limit > 10000:
                msg = 'limit value should be less than or equal to 10000'
                return self.get_exception(
                    400, headers, request.format, 'InvalidParameterValue', msg)
        except TypeError as err:
            LOGGER.warning(err)
            limit = int(self.config['server']['limit'])
        except ValueError:
            msg = 'limit value should be an integer'
            return self.get_exception(
                400, headers, request.format, 'InvalidParameterValue', msg)

        LOGGER.debug('Processing datetime parameter')
        datetime_ = request.params.get('datetime')
        try:
            datetime_ = validate_datetime(datetime_)
        except ValueError as err:
            msg = str(err)
            return self.get_exception(
                400, headers, request.format, 'InvalidParameterValue', msg)

        subTemporalValue = request.params.get('subTemporalValue')
        if subTemporalValue is None:
            subTemporalValue = False

        LOGGER.debug('Querying provider')
        LOGGER.debug('offset: {}'.format(offset))
        LOGGER.debug('limit: {}'.format(limit))
        LOGGER.debug('datetime: {}'.format(datetime_))
        
        pd = ProcessMobilityData()
        content = {
            "temporalProperties": [],
            "links":[]
        }

        try:              
            pd.connect()
            rows, numberMatched, numberReturned = pd.getTemporalProperties(collection_id=collection_id,mfeature_id=mfeature_id,datetime=datetime_,limit=limit,offset=offset,subTemporalValue=subTemporalValue)
            
            temporalProperties = []
            if subTemporalValue == False or subTemporalValue == "false":
                for row in rows:    
                    temporalProperty = row[3]
                    temporalProperty['name'] = row[2]

                    temporalProperties.append(temporalProperty)
            else:                          
                split_groups = {}
                for i in range(len(rows)): 
                    group_id = str(rows[i][4])                    
                    if group_id not in split_groups:
                        split_groups[group_id] = []
                    split_groups[group_id].append(i)                
                pymeos_initialize()  
                for key, group_row_index in split_groups.items():  
                    group = {}
                    group["datetimes"] = []
                    for row_index in group_row_index:
                        row = rows[int(row_index)]
                        tproperties_name = row[2]
                        group[tproperties_name] = row[3] if row[3] is not None else {}
                        if row[5] is not None or row[6] is not None:
                            temporalPropertyValue = Temporal.as_mfjson(TFloatSeq(str(row[5]).replace("'","")), False) if row[5] != None else Temporal.as_mfjson(TTextSeq(str(row[6]).replace("'","")), False)
                            temporalPropertyValue = pd.convertTemporalPropertyValueToBaseVersion(json.loads(temporalPropertyValue))
                                    
                            if 'datetimes' in temporalPropertyValue:
                                group["datetimes"] = temporalPropertyValue.pop("datetimes", None) 
                            group[tproperties_name].update(temporalPropertyValue)
                    temporalProperties.append(group)
            content["temporalProperties"] = temporalProperties
        except (Exception, psycopg2.Error) as error:
            msg = str(error)
            return self.get_exception(
            400, headers, request.format, 'ConnectingError', msg) 

        # TODO: translate titles
        uri = '{}/{}/items/{}/tProperties'.format(self.get_collections_url(), collection_id, mfeature_id)        
        
        serialized_query_params = ''
        for k, v in request.params.items():
            if k not in ('f', 'offset'):
                serialized_query_params += '&'
                serialized_query_params += urllib.parse.quote(k, safe='')
                serialized_query_params += '='
                serialized_query_params += urllib.parse.quote(str(v), safe=',')

        content['links'] = [{
                'href': '{}?offset={}{}'.format(uri, offset, serialized_query_params),
                'rel': request.get_linkrel(F_JSON),
                'type': FORMAT_TYPES[F_JSON]
            }]
        
        if len(content['temporalProperties']) == limit:
            next_ = offset + limit
            content['links'].append(
                {
                    'href': '{}?offset={}{}'.format( uri, next_, serialized_query_params),
                    'type': 'application/geo+json',
                    'rel': 'next',
                })

        content['timeStamp'] = datetime.utcnow().strftime(
            '%Y-%m-%dT%H:%M:%S.%fZ')

        content['numberMatched'] = numberMatched
        content['numberReturned'] = numberReturned
        return headers, 200, to_json(content, self.pretty_print)

    @gzip
    @pre_process
    def manage_collection_item_tProperty(
            self, request: Union[APIRequest, Any],
            action, dataset, identifier, tProperty=None) -> Tuple[dict, int, str]:
        """
        Adds Temporal Property item to a moving feature

        :param request: A request object
        :param dataset: dataset name
        :param identifier: moving feature's id 
        :param tProperty: Temporal Property's id 

        :returns: tuple of headers, status code, content
        """

        if not request.is_valid(PLUGINS['formatter'].keys()):
            return self.get_format_exception(request)

        # Set Content-Language to system locale until provider locale
        # has been determined
        headers = request.get_response_headers(SYSTEM_LOCALE)

        pd = ProcessMobilityData()
        excuted, featureList = getListOfFeaturesId()

        if excuted == False:
            msg = str(featureList)
            return self.get_exception(
            400, headers, request.format, 'ConnectingError', msg)   

        if [dataset, identifier] not in featureList:
            msg = 'Feature not found'
            LOGGER.error(msg)
            return self.get_exception(
                404, headers, request.format, 'NotFound', msg)

        collectionId = dataset
        mfeature_id = identifier
        tPropertiesName = tProperty
        if action == 'create':
            if not request.data:
                msg = 'No data found'
                LOGGER.error(msg)
                return self.get_exception(
                    400, headers, request.format, 'InvalidParameterValue', msg)
            
            data = request.data
            try:
                # Parse bytes data, if applicable
                data = data.decode()
                LOGGER.debug(data)
            except (UnicodeDecodeError, AttributeError):
                pass

            try:
                data = json.loads(data)
            except (json.decoder.JSONDecodeError, TypeError) as err:
                # Input does not appear to be valid JSON
                LOGGER.error(err)
                msg = 'invalid request data'
                return self.get_exception(
                    400, headers, request.format, 'InvalidParameterValue', msg)
            
            if checkRequiredFieldTemporalProperties(data) == False:
                # TODO not all processes require input
                msg = 'The required tag (e.g., type,temporalgeometry) is missing from the request data.'
                return self.get_exception(
                    501, headers, request.format, 'MissingParameterValue', msg)
            
            LOGGER.debug('Creating item')
            try:
                pd.connect()   
                temporalProperties = data['temporalProperties']
                temporalProperties = [temporalProperties] if not isinstance(temporalProperties, list) else temporalProperties

                canPost = pd.checkIfTemporalPropertyCanPost(collectionId, mfeature_id, temporalProperties) 

                if canPost == True:
                    for temporalProperty in temporalProperties:
                        tPropertiesName = pd.postTemporalProperties(collectionId, mfeature_id, temporalProperty)
                else :
                    return headers, 400, ''
            except (Exception, psycopg2.Error) as error:
                msg = str(error)
                return self.get_exception(
                400, headers, request.format, 'ConnectingError', msg)    
            finally:
                pd.disconnect() 

            headers['Location'] = '{}/{}/items/{}/tProperties/{}'.format(
                self.get_collections_url(), dataset, mfeature_id, tPropertiesName)

            return headers, 201, ''      
        
        if action == 'delete':
            LOGGER.debug('Deleting item')

            try:
                pd.connect()   
                pd.deleteTemporalProperties("AND tproperties_name ='{0}'".format(tPropertiesName))
                
            except (Exception, psycopg2.Error) as error:
                msg = str(error)
                return self.get_exception(
                400, headers, request.format, 'ConnectingError', msg)    
            finally:
                pd.disconnect()
            
            return headers, 204, ''
        
    @gzip
    @pre_process
    def get_collection_items_tProperty_value(self, request: Union[APIRequest, Any],
                            dataset, identifier, tProperty) -> Tuple[dict, int, str]:
        """
        Get temporal Properties of collection item

        :param request: A request object
        :param dataset: dataset name
        :param identifier: item identifier
        :param tProperty: Temporal Property

        :returns: tuple of headers, status code, content
        """

        if not request.is_valid():
            return self.get_format_exception(request)
        headers = request.get_response_headers(SYSTEM_LOCALE)

        excuted, tPropertyList = getListOftPropertiesName()
        if excuted == False:
            msg = str(tPropertyList)
            return self.get_exception(
            400, headers, request.format, 'ConnectingError', msg)   

        if [dataset, identifier, tProperty] not in tPropertyList:
            msg = 'Temporal Property not found'
            LOGGER.error(msg)
            return self.get_exception(
                404, headers, request.format, 'NotFound', msg)     

        collection_id = dataset
        mfeature_id = identifier
        tProperty_name = tProperty
        LOGGER.debug('Processing query parameters')

        LOGGER.debug('Processing offset parameter')
        try:
            offset = int(request.params.get('offset'))
            if offset < 0:
                msg = 'offset value should be positive or zero'
                return self.get_exception(
                    400, headers, request.format, 'InvalidParameterValue', msg)
        except TypeError as err:
            LOGGER.warning(err)
            offset = 0
        except ValueError:
            msg = 'offset value should be an integer'
            return self.get_exception(
                400, headers, request.format, 'InvalidParameterValue', msg)

        LOGGER.debug('Processing limit parameter')
        try:
            limit = int(request.params.get('limit'))
            # TODO: We should do more validation, against the min and max
            #       allowed by the server configuration
            if limit <= 0:
                msg = 'limit value should be strictly positive'
                return self.get_exception(
                    400, headers, request.format, 'InvalidParameterValue', msg)
            if limit > 10000:
                msg = 'limit value should be less than or equal to 10000'
                return self.get_exception(
                    400, headers, request.format, 'InvalidParameterValue', msg)
        except TypeError as err:
            LOGGER.warning(err)
            limit = int(self.config['server']['limit'])
        except ValueError:
            msg = 'limit value should be an integer'
            return self.get_exception(
                400, headers, request.format, 'InvalidParameterValue', msg)

        LOGGER.debug('Processing leaf parameter')
        leaf_ = request.params.get('leaf')
        try:
            leaf_ = validate_leaf(leaf_)
        except ValueError as err:
            msg = str(err)
            return self.get_exception(
                400, headers, request.format, 'InvalidParameterValue', msg)

        subTemporalValue = request.params.get('subTemporalValue')
        if subTemporalValue is None:
            subTemporalValue = False        

        if (leaf_ != '' and leaf_ is not None) and (subTemporalValue == True or subTemporalValue == 'true'):
            msg = 'Cannot use both parameter `subTemporalValue` and `leaf` at the same time'
            return self.get_exception(
                400, headers, request.format, 'InvalidParameterValue', msg)
        
        LOGGER.debug('Processing datetime parameter')
        datetime_ = request.params.get('datetime')
        try:
            datetime_ = validate_datetime(datetime_)
        except ValueError as err:
            msg = str(err)
            return self.get_exception(
                400, headers, request.format, 'InvalidParameterValue', msg)

        LOGGER.debug('Querying provider')
        LOGGER.debug('offset: {}'.format(offset))
        LOGGER.debug('limit: {}'.format(limit))
        LOGGER.debug('leaf: {}'.format(leaf_))
        LOGGER.debug('datetime: {}'.format(datetime_))
        
        pd = ProcessMobilityData()
        content = {}

        try:              
            pd.connect()
            rows = pd.getTemporalPropertiesValue(collection_id=collection_id,mfeature_id=mfeature_id,tProperty_name=tProperty_name, leaf=leaf_,datetime=datetime_,subTemporalValue=subTemporalValue)
            pymeos_initialize()
            valueSequence = []
            for row in rows: 
                content = row[3]  
                if row[5] is not None or row[6] is not None: 
                    temporalPropertyValue = Temporal.as_mfjson(TFloatSeq(str(row[5]).replace("'","")), False) if row[5] != None else Temporal.as_mfjson(TTextSeq(str(row[6]).replace("'","")), False)
                    valueSequence.append(pd.convertTemporalPropertyValueToBaseVersion(json.loads(temporalPropertyValue)))
            content["valueSequence"] = valueSequence
        except (Exception, psycopg2.Error) as error:
            msg = str(error)
            return self.get_exception(
            400, headers, request.format, 'ConnectingError', msg) 

        # TODO: translate titles
        return headers, 200, to_json(content, self.pretty_print)

    @gzip
    @pre_process
    def manage_collection_item_tProperty_value(
            self, request: Union[APIRequest, Any],
            action, dataset, identifier, tProperty=None) -> Tuple[dict, int, str]:
        """
        Adds Temporal Property Value item to a Temporal Property

        :param request: A request object
        :param dataset: dataset name
        :param identifier: moving feature's id 
        :param tProperty: Temporal Property's id 

        :returns: tuple of headers, status code, content
        """

        if not request.is_valid(PLUGINS['formatter'].keys()):
            return self.get_format_exception(request)

        # Set Content-Language to system locale until provider locale
        # has been determined
        headers = request.get_response_headers(SYSTEM_LOCALE)

        pd = ProcessMobilityData()
        excuted, tPropertyList = getListOftPropertiesName()
        if excuted == False:
            msg = str(tPropertyList)
            return self.get_exception(
            400, headers, request.format, 'ConnectingError', msg)   

        if [dataset, identifier, tProperty] not in tPropertyList:
            msg = 'Temporal Property not found'
            LOGGER.error(msg)
            return self.get_exception(
                404, headers, request.format, 'NotFound', msg)

        collectionId = dataset
        mfeature_id = identifier
        tProperty_name = tProperty
        if action == 'create':
            if not request.data:
                msg = 'No data found'
                LOGGER.error(msg)
                return self.get_exception(
                    400, headers, request.format, 'InvalidParameterValue', msg)
            
            data = request.data
            try:
                # Parse bytes data, if applicable
                data = data.decode()
                LOGGER.debug(data)
            except (UnicodeDecodeError, AttributeError):
                pass

            try:
                data = json.loads(data)
            except (json.decoder.JSONDecodeError, TypeError) as err:
                # Input does not appear to be valid JSON
                LOGGER.error(err)
                msg = 'invalid request data'
                return self.get_exception(
                    400, headers, request.format, 'InvalidParameterValue', msg)
            
            if checkRequiredFieldTemporalValue(data) == False:
                # TODO not all processes require input
                msg = 'The required tag (e.g., type,temporalgeometry) is missing from the request data.'
                return self.get_exception(
                    501, headers, request.format, 'MissingParameterValue', msg)
            
            LOGGER.debug('Creating item')
            try:
                pd.connect()      
                canPost = pd.checkIfTemporalPropertyCanPost(collectionId, mfeature_id, [data], tProperty_name) 
                if canPost == True:
                    pValue_id = pd.postTemporalValue(collectionId, mfeature_id, tProperty_name, data)
                else :
                    return headers, 400, ''
            except (Exception, psycopg2.Error) as error:
                msg = str(error)
                return self.get_exception(
                400, headers, request.format, 'ConnectingError', msg)    
            finally:
                pd.disconnect() 
            headers['Location'] = '{}/{}/items/{}/tProperties/{}/pvalue/{}'.format(
                self.get_collections_url(), dataset, mfeature_id, tProperty_name, pValue_id)

            return headers, 201, ''          

    def get_exception(self, status, headers, format_, code,
                    description) -> Tuple[dict, int, str]:
        """
        Exception handler

        :param status: HTTP status code
        :param headers: dict of HTTP response headers
        :param format_: format string
        :param code: OGC API exception code
        :param description: OGC API exception code

        :returns: tuple of headers, status, and message
        """

        LOGGER.error(description)
        exception = {
            'code': code,
            'description': description
        }

        if format_ == F_HTML:
            headers['Content-Type'] = FORMAT_TYPES[F_HTML]
            content = render_j2_template(
                self.config, 'exception.html', exception, SYSTEM_LOCALE)
        else:
            content = to_json(exception, self.pretty_print)

        return headers, status, content

    def get_format_exception(self, request) -> Tuple[dict, int, str]:
        """
        Returns a format exception.

        :param request: An APIRequest instance.

        :returns: tuple of (headers, status, message)
        """

        # Content-Language is in the system locale (ignore language settings)
        headers = request.get_response_headers(SYSTEM_LOCALE)
        msg = f'Invalid format: {request.format}'
        return self.get_exception(
            400, headers, request.format, 'InvalidParameterValue', msg)

    def get_collections_url(self):
        return '{}/collections'.format(self.config['server']['url'])


def validate_bbox(value=None) -> list:
    """
    Helper function to validate bbox parameter

    :param value: `list` of minx, miny, maxx, maxy

    :returns: bbox as `list` of `float` values
    """

    if value is None:
        LOGGER.debug('bbox is empty')
        return []

    bbox = value.split(',')

    if len(bbox) != 4 and len(bbox) != 6:
        msg = 'bbox should be 4 values (minx,miny,maxx,maxy) or 6 values (minx,miny,minz,maxx,maxy,maxz)'
        LOGGER.debug(msg)
        raise ValueError(msg)

    try:
        bbox = [float(c) for c in bbox]
    except ValueError as err:
        msg = 'bbox values must be numbers'
        err.args = (msg,)
        LOGGER.debug(msg)
        raise

    if len(bbox) == 4:
        if bbox[1] > bbox[3]:
            msg = 'miny should be less than maxy'
            LOGGER.debug(msg)
            raise ValueError(msg)

        if bbox[0] > bbox[2]:
            msg = 'minx is greater than maxx (possibly antimeridian bbox)'
            LOGGER.debug(msg)
            raise ValueError(msg)

    if len(bbox) == 6:
        if bbox[2] > bbox[5]:
            msg = 'minz should be less than maxz'
            LOGGER.debug(msg)
            raise ValueError(msg)
        
        if bbox[1] > bbox[4]:
            msg = 'miny should be less than maxy'
            LOGGER.debug(msg)
            raise ValueError(msg)

        if bbox[0] > bbox[3]:
            msg = 'minx is greater than maxx (possibly antimeridian bbox)'
            LOGGER.debug(msg)
            raise ValueError(msg)
    
    return bbox

def validate_leaf(leaf_=None) -> str:
    """
    Helper function to validate temporal parameter

    :param resource_def: `dict` of configuration resource definition
    :param datetime_: `str` of datetime parameter

    :returns: `str` of datetime input, if valid
    """

    # TODO: pass datetime to query as a `datetime` object
    # we would need to ensure partial dates work accordingly
    # as well as setting '..' values to `None` so that underlying
    # providers can just assume a `datetime.datetime` object
    #
    # NOTE: needs testing when passing partials from API to backend

    unix_epoch = datetime(1970, 1, 1, 0, 0, 0)
    dateparse_ = partial(dateparse, default=unix_epoch)

    leaf_invalid = False

    if leaf_ is not None:        
        LOGGER.debug('detected leaf_')
        LOGGER.debug('Validating time windows')
        leaf_list = leaf_.split(',')

        leaf_ = ''
        if(len(leaf_list) > 0):                
            datetime_ = dateparse_(leaf_list[0])
            leaf_ = datetime_.strftime('%Y-%m-%d %H:%M:%S.%f')
            
        for i in range(1, len(leaf_list)):            
            datetime_Pre = dateparse_(leaf_list[i - 1])          
            datetime_ = dateparse_(leaf_list[i])
            
            if datetime_Pre != '..':
                if datetime_Pre.tzinfo is None:
                    datetime_Pre = datetime_Pre.replace(tzinfo=pytz.UTC)

            if datetime_!= '..':
                if datetime_.tzinfo is None:
                    datetime_ = datetime_.replace(tzinfo=pytz.UTC)

            if datetime_Pre >= datetime_:
                leaf_invalid = True
                break        
            leaf_ += ',' + datetime_.strftime('%Y-%m-%d %H:%M:%S.%f') 
        
    if leaf_invalid:
        msg = 'invalid leaf'
        LOGGER.debug(msg)
        raise ValueError(msg)
    return leaf_

def validate_datetime(datetime_=None) -> str:
    """
    Helper function to validate temporal parameter

    :param resource_def: `dict` of configuration resource definition
    :param datetime_: `str` of datetime parameter

    :returns: `str` of datetime input, if valid
    """

    # TODO: pass datetime to query as a `datetime` object
    # we would need to ensure partial dates work accordingly
    # as well as setting '..' values to `None` so that underlying
    # providers can just assume a `datetime.datetime` object
    #
    # NOTE: needs testing when passing partials from API to backend

    datetime_invalid = False

    if datetime_ is not None and datetime_ != '':

        dateparse_begin = partial(dateparse, default=datetime.min)
        dateparse_end = partial(dateparse, default=datetime.max)
        unix_epoch = datetime(1970, 1, 1, 0, 0, 0)
        dateparse_ = partial(dateparse, default=unix_epoch)

        if '/' in datetime_:  # envelope
            LOGGER.debug('detected time range')
            LOGGER.debug('Validating time windows')

            # normalize "" to ".." (actually changes datetime_)
            datetime_ = re.sub(r'^/', '../', datetime_)
            datetime_ = re.sub(r'/$', '/..', datetime_)

            datetime_begin, datetime_end = datetime_.split('/')
            if datetime_begin != '..':
                datetime_begin = dateparse_begin(datetime_begin)
                if datetime_begin.tzinfo is None:
                    datetime_begin = datetime_begin.replace(
                        tzinfo=pytz.UTC)
            else:
                datetime_begin = datetime(1, 1, 1, 0, 0, 0).replace(
                        tzinfo=pytz.UTC)

            if datetime_end != '..':
                datetime_end = dateparse_end(datetime_end)
                if datetime_end.tzinfo is None:
                    datetime_end = datetime_end.replace(tzinfo=pytz.UTC)
            else:
                datetime_end = datetime(9999, 1, 1, 0, 0, 0).replace(
                        tzinfo=pytz.UTC)

            datetime_invalid = any([
                (datetime_begin > datetime_end)
            ])
            datetime_ = datetime_begin.strftime('%Y-%m-%d %H:%M:%S.%f') + ',' +  datetime_end.strftime('%Y-%m-%d %H:%M:%S.%f')
        else:  # time instant
            LOGGER.debug('detected time instant')
            datetime__ = dateparse_(datetime_)
            if datetime__ != '..':
                if datetime__.tzinfo is None:
                    datetime__ = datetime__.replace(tzinfo=pytz.UTC)
            datetime_invalid = any([
                (datetime__ == '..')
            ])
            datetime_ = datetime__.strftime('%Y-%m-%d %H:%M:%S.%f') + ',' + datetime__.strftime('%Y-%m-%d %H:%M:%S.%f')

    if datetime_invalid:
        msg = 'datetime parameter out of range'
        LOGGER.debug(msg)
        raise ValueError(msg)
    return datetime_

def getListOfCollectionsId():  
    pd = ProcessMobilityData()
    try:
        pd.connect()       
        rows = pd.getCollectionsList()
        CollectionsId = []
        for row in rows:
            CollectionsId.append(row[0])
        return True, CollectionsId
    except (Exception, psycopg2.Error) as error:
        return False, error  
    finally:
        pd.disconnect()


def getListOfFeaturesId():  
    pd = ProcessMobilityData()
    try:
        pd.connect()       
        rows = pd.getFeaturesList()
        FeaturesList = []
        for row in rows:
            FeaturesList.append([row[0], row[1]])
        return True, FeaturesList
    except (Exception, psycopg2.Error) as error:
        return False, error  
    finally:
        pd.disconnect()

def getListOftPropertiesName():  
    pd = ProcessMobilityData()
    try:
        pd.connect()       
        rows = pd.gettPropertiesNameList()
        tPropertiesNameList = []
        for row in rows:
            tPropertiesNameList.append([row[0], row[1], row[2]])
        return True, tPropertiesNameList
    except (Exception, psycopg2.Error) as error:
        return False, error  
    finally:
        pd.disconnect()

def checkRequiredFieldFeature(feature):
    if ('type' not in feature
        or 'temporalGeometry' not in feature):
        return False    
    if checkRequiredFieldTemporalGeometries(feature['temporalGeometry']) == False :
        return False    
    if 'temporalProperties' in feature:
        if checkRequiredFieldTemporalProperty(feature['temporalProperties']) == False:  
            return False     
    if 'geometry' in feature:
        if checkRequiredFieldGeometries(feature['geometry']) == False :   
            return False     
    if 'crs' in feature:
        if checkRequiredFieldCrs(feature['crs']) == False:     
            return False    
    if 'trs' in feature:
        if checkRequiredFieldCrs(feature['trs']) == False: 
            return False
    return True

def checkRequiredFieldGeometries(geometry):
    if (checkRequiredFieldGeometry_Array(geometry) == False 
        and checkRequiredFieldGeometry_Single(geometry) == False):
        return False     
    return True

def checkRequiredFieldGeometry_Array(geometry):
    if ('type' not in geometry
        or 'geometries' not in geometry):
        return False
    geometries = geometry['geometries']    
    geometries = [geometries] if not isinstance(geometries, list) else geometries    
    for l_geometry in geometries:
        if checkRequiredFieldGeometry_Single(l_geometry) == False:
            return False    
    return True

def checkRequiredFieldGeometry_Single(geometry):
    if ('type' not in geometry
        or 'coordinates' not in geometry):
        return False
    return True

def checkRequiredFieldTemporalGeometries(temporalGeometries):    
    if (checkRequiredFieldTemporalGeometry_Array(temporalGeometries) == False 
        and checkRequiredFieldTemporalGeometry_Single(temporalGeometries) == False):
        return False
    return True

def checkRequiredFieldTemporalGeometry_Array(temporalGeometries):
    if ('type' not in temporalGeometries
        or 'prisms' not in temporalGeometries):
        return False
    prisms = temporalGeometries['prisms']    
    prisms = [prisms] if not isinstance(prisms, list) else prisms    
    for temporalGeometry in prisms:
        if checkRequiredFieldTemporalGeometry_Single(temporalGeometry) == False:
            return False    
    if 'crs' in temporalGeometries:
        if checkRequiredFieldCrs(temporalGeometry['crs']) == False:
            return False    
    if 'trs' in temporalGeometries:
        if checkRequiredFieldCrs(temporalGeometry['trs']) == False:
            return False
    return True

def checkRequiredFieldTemporalGeometry_Single(temporalGeometry):
    if ('type' not in temporalGeometry
        or 'datetimes' not in temporalGeometry
        or 'coordinates' not in temporalGeometry):
        return False
    if 'crs' in temporalGeometry:
        if checkRequiredFieldCrs(temporalGeometry['crs']) == False:
            return False    
    if 'trs' in temporalGeometry:
        if checkRequiredFieldCrs(temporalGeometry['trs']) == False:
            return False
    return True

def checkRequiredFieldTemporalProperties(temporalProperties):
    if 'temporalProperties' not in temporalProperties:
        return False    
    if checkRequiredFieldTemporalProperty(temporalProperties['temporalProperties'])== False:
        return False
    return True

def checkRequiredFieldTemporalProperty(temporalProperties):  
    temporalProperties = [temporalProperties] if not isinstance(temporalProperties, list) else temporalProperties
    for temporalProperty in temporalProperties:
        if ('datetimes' not in temporalProperty):            
            return False    
        for tproperties_name in temporalProperty:
            if  tproperties_name != 'datetimes' and ('values' not in temporalProperty[tproperties_name] or 'interpolation' not in temporalProperty[tproperties_name]):
                return False    
    return True

def checkRequiredFieldTemporalValue(temporalValue):
    if ('datetimes' not in temporalValue
        or 'values' not in temporalValue
        or 'interpolation' not in temporalValue):
        return False
    return True

def checkRequiredFieldCrs(crs):
    if ('type' not in crs
        or 'properties' not in crs):
        return False
    return True

def checkRequiredFieldTrs(trs):
    if ('type' not in trs
        or 'properties' not in trs):
        return False
    return True
