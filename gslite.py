#!/usr/bin/env python
#
# Copyright 2010 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

"""Simple, extendable, mockable Python client for Google Storage.

This module only depends on standard Python libraries. It is intended to provide
a set of base client classes with all critical features implemented. Advanced
features can be added by extending the classes. Or, it can be used as-is.

Installation:
  Put this script in your python path.

Usage:
  1) Get a Google Storage account and credentials.
  2) Put this script in your Python path.
  2) Decide how you will store your credentials (private file, environment
     variables, etc...).
  3) Create a GsClient or child instance, passing credentials to constructor.
  4) Use the relevant functions on the client

URL Encoding:
  Users of this module do not need to URL encode/decode any request arguments
  or response results.
  Object names and query parameters may contain characters that are illegal
  URL characters. So, all object name and query parameter values are
  percent encoded by this module before sending the request. This is important
  to understand since you do not want to encode your strings twice.
  It is also important to understand that all object names and prefixes
  found in ListBucketResult responses will not be encoded.

Handling Errors:
  Google Storage service errors will be raised as GsError exceptions.
  Other connection errors may get raised as httplib.HTTPException errors.

Windows Considerations:
  When opening files, you must specify binary mode, like this:
    infile = open(filename, 'rb')
    outfile = open(filename, 'wb')

Example where credentials are in GS_ACCESS and GS_SECRET env vars:
$ python
>>> import os
>>> import gslite
>>> gs_access = os.environ['GS_ACCESS']
>>> gs_secret = os.environ['GS_SECRET']
>>> bucket = 'my_super_cool_bucket_name'
>>> filename = 'hello.txt'
>>> client = gslite.GsClient(access_key=gs_access, secret=gs_secret)
>>> client.put_bucket(bucket)
>>> infile = open(filename)
>>> client.put_object(bucket, filename, infile)
>>> infile.close()
>>> client.get_bucket(bucket).get_keys()
['hello.txt']
>>> client.delete_object(bucket, filename)
>>> client.delete_bucket(bucket)
"""

__version__ = '1.0'

import base64
import hashlib
import hmac
import httplib
import logging
import os
import StringIO
import time
import urllib
import urlparse
import xml.dom.minidom

# Success and retryable status codes.
REDIRECT_CODES = (301, 302, 303, 307)
DEFAULT_SUCCESS_CODES = (200,)
DEFAULT_RETRYABLE_CODES = (408, 500, 502, 503, 504)
GET_OBJECT_SUCCESS_CODES = (200, 206)
DEL_BUCKET_SUCCESS_CODES = (204,)
DEL_BUCKET_RETRYABLE_CODES = (404, 408, 409, 500, 502, 503, 504)
DEL_OBJECT_SUCCESS_CODES = (204,)


class GsError(Exception):
  """Base error for all client errors.

  Instance data:
    msg: error message
    operations: list of operations associated with error
  """

  def __init__(self, msg, operations):
    """GsError constructor.

    Args:
      msg: message string
      operations: list of operations associated with error.
    """
    self.msg = msg
    self.operations = operations

  def __str__(self):
    """Convert instance to loggable string."""
    s = StringIO.StringIO()
    s.write('GsError: %s' % self.msg)
    for i in xrange(len(self.operations)):
      s.write('\n\nOPERATION %d:' % i)
      s.write('\n%s' % self.operations[i])
    return s.getvalue()


class GsXmlBase(object):
  """Base XML oject parser/generator."""

  @staticmethod
  def value_from_elems(elems):
    """Returns a child node text value in the last element in elems.

    Args:
      elems: A list of Element objects from the xml.dom.minidom module.

    Returns:
      String value of last node or empty string if not found.
    """
    ret = ''
    if elems:
      child_nodes = elems[-1].childNodes
      if child_nodes:
        ret = child_nodes[-1].nodeValue
    return str(ret)

  @staticmethod
  def add_text_node(dom, parent_node, node_name, node_text):
    """Adds a simple text node to a parent node.

    Args:
      dom: dom object from xml.dom.minidom module.
      parent_node: Parent Node object from the xml.dom.minidom module.
      node_name: Name of new child node
      node_text: Text content of new node.
    """
    elem = dom.createElement(node_name)
    text = dom.createTextNode(node_text)
    elem.appendChild(text)
    parent_node.appendChild(elem)


class GsAccessControlList(GsXmlBase):
  """AccessControlList XML parser/generator.

  See the Google Storage API documentation for more information about the
  AccessControlList XML specification.

  Instance data:
    owner_id: owner google storage id as string
    owner_name: owner name as string
    entries: list of GsAccessControlList.Entry instances
  """

  class Entry(object):
    """Entry class corresponding to like named element.

    Instance data:
      permission: permission as string ('READ', 'WRITE', etc...)
      scope_type: scope type as string ('UserById', etc...)
      scope_user_id: scope user google storage id as string
      scope_user_name: scope user name as string
      scope_email: scope user email address as string
      scope_domain: scope domain as string
    """

    def __init__(self,
                 permission='',
                 scope_type='',
                 scope_user_id='',
                 scope_user_name='',
                 scope_email='',
                 scope_domain=''):
      """Entry Constructor.

      Args:
        permission: permission as string ('READ', 'WRITE', etc...)
        scope_type: scope type as string ('UserById', etc...)
        scope_user_id: scope user google storage id as string
        scope_user_name: scope user name as string
        scope_email: scope user email address as string
        scope_domain: scope domain as string
      """
      self.permission = permission
      self.scope_type = scope_type
      self.scope_user_id = scope_user_id
      self.scope_user_name = scope_user_name
      self.scope_email = scope_email
      self.scope_domain = scope_domain

  def __init__(self, owner_id='', owner_name=''):
    """GsAccessControlList Constructor.

    Args:
      owner_id: owner google storage id as string
      owner_name: owner name as string
    """
    self.owner_id = owner_id
    self.owner_name = owner_name
    self.entries = []

  def add_entry(self,
                permission='',
                scope_type='',
                scope_user_id='',
                scope_user_name='',
                scope_email='',
                scope_domain=''):
    """Adds an entry to the acl.

    Args:
      permission: permission as string ('READ', 'WRITE', etc...)
      scope_type: scope type as string ('UserById', etc...)
      scope_user_id: scope user google storage id as string
      scope_user_name: scope user name as string
      scope_email: scope user email address as string
      scope_domain: scope domain as string
    """
    self.entries.append(GsAccessControlList.Entry(
        permission=permission,
        scope_type=scope_type,
        scope_user_id=scope_user_id,
        scope_user_name=scope_user_name,
        scope_email=scope_email,
        scope_domain=scope_domain))

  def parse_xml(self, xml_str):
    """Parses the given xml string to this object.

    Args:
      xml_str: AccessControlList XML as string
    """
    self.owner_id = ''
    self.owner_name = ''
    self.entries = []
    dom = xml.dom.minidom.parseString(xml_str)
    owner_elems = dom.getElementsByTagName('Owner')
    for owner_elem in owner_elems:
      self.owner_id = self.value_from_elems(
          owner_elem.getElementsByTagName('ID'))
      self.owner_name = self.value_from_elems(
          owner_elem.getElementsByTagName('Name'))
    entries_elems = dom.getElementsByTagName('Entries')
    for entries_elem in entries_elems:
      entry_elems = entries_elem.getElementsByTagName('Entry')
      for entry_elem in entry_elems:
        entry = GsAccessControlList.Entry()
        entry.permission = self.value_from_elems(
            entry_elem.getElementsByTagName('Permission'))
        scope_elems = entry_elem.getElementsByTagName('Scope')
        for scope_elem in scope_elems:
          entry.scope_type = scope_elem.getAttribute('type')
          entry.scope_user_id = self.value_from_elems(
              scope_elem.getElementsByTagName('ID'))
          entry.scope_user_name = self.value_from_elems(
              scope_elem.getElementsByTagName('Name'))
          entry.scope_email = self.value_from_elems(
              scope_elem.getElementsByTagName('EmailAddress'))
          entry.scope_domain = self.value_from_elems(
              scope_elem.getElementsByTagName('Domain'))
        self.entries.append(entry)

  def to_xml(self, pretty=False):
    """Translates this acl object to XML string.

    Args:
      pretty: if True, output will use dom.toprettyxml

    Returns:
      AccessControlList XML as string
    """
    impl = xml.dom.minidom.getDOMImplementation()
    dom = impl.createDocument(None, 'AccessControlList', None)
    top_elem = dom.documentElement
    if self.owner_id or self.owner_name:
      owner_elem = dom.createElement('Owner')
      if self.owner_id:
        self.add_text_node(dom, owner_elem, 'ID', self.owner_id)
      if self.owner_name:
        self.add_text_node(dom, owner_elem, 'Name', self.owner_name)
      top_elem.appendChild(owner_elem)
    if self.entries:
      entries_elem = dom.createElement('Entries')
      for entry in self.entries:
        entry_elem = dom.createElement('Entry')
        if entry.permission:
          self.add_text_node(dom,
                             entry_elem,
                             'Permission',
                             entry.permission)
        if (entry.scope_type or
            entry.scope_user_id or
            entry.scope_user_name or
            entry.scope_email or
            entry.scope_domain):
          scope_elem = dom.createElement('Scope')
          if entry.scope_type:
            scope_elem.setAttribute('type', entry.scope_type)
          if entry.scope_user_id:
            self.add_text_node(dom,
                               scope_elem,
                               'ID',
                               entry.scope_user_id)
          if entry.scope_user_name:
            self.add_text_node(dom,
                               scope_elem,
                               'Name',
                               entry.scope_user_name)
          if entry.scope_email:
            self.add_text_node(dom,
                               scope_elem,
                               'EmailAddress',
                               entry.scope_email)
          if entry.scope_domain:
            self.add_text_node(dom,
                               scope_elem,
                               'Domain',
                               entry.scope_domain)
          entry_elem.appendChild(scope_elem)
        entries_elem.appendChild(entry_elem)
      top_elem.appendChild(entries_elem)
    if pretty:
      return dom.toprettyxml(indent='  ')
    return dom.toxml()


class GsListAllMyBucketsResult(GsXmlBase):
  """ListAllMyBucketsResult XML parser.

  See the Google Storage API documentation for more information about the
  ListAllMyBucketsResult XML specification.

  Instance data:
    owner_id: owner google storage id as string
    owner_display_name: owner name as string
    bucket_list: list of GsListAllMyBucketsResult.Bucket instances
  """

  class Bucket(object):
    """Bucket class corresponding to like named element.

    Instance data:
      name: bucket name as string
      creation_date: bucket creation date as string
    """

    def __init__(self):
      """Bucket constructor."""
      self.name = ''
      self.creation_date = ''

  def __init__(self):
    """GsListAllMyBucketsResult constructor."""
    self.owner_id = ''
    self.owner_display_name = ''
    self.bucket_list = []

  def parse_xml(self, xml_str):
    """Parses the given xml string to this object.

    Args:
      xml_str: ListAllMyBucketsResult XML as string
    """
    self.owner_id = ''
    self.owner_display_name = ''
    self.bucket_list = []
    dom = xml.dom.minidom.parseString(xml_str)
    owner_elems = dom.getElementsByTagName('Owner')
    for owner_elem in owner_elems:
      self.owner_id = self.value_from_elems(
          owner_elem.getElementsByTagName('ID'))
      self.owner_display_name = self.value_from_elems(
          owner_elem.getElementsByTagName('DisplayName'))
    buckets_elems = dom.getElementsByTagName('Buckets')
    for buckets_elem in buckets_elems:
      bucket_elems = buckets_elem.getElementsByTagName('Bucket')
      for bucket_elem in bucket_elems:
        bucket = GsListAllMyBucketsResult.Bucket()
        bucket.name = self.value_from_elems(
            bucket_elem.getElementsByTagName('Name'))
        bucket.creation_date = self.value_from_elems(
            bucket_elem.getElementsByTagName('CreationDate'))
        self.bucket_list.append(bucket)

  def get_bucket_names(self):
    """Returns the list of bucket names from self.bucket_list."""
    return [b.name for b in self.bucket_list]


class GsListBucketResult(GsXmlBase):
  """ListBucketResult XML parser.

  See the Google Storage API documentation for more information about the
  ListBucketResult XML specification.

  Instance data:
    name: bucket name as string
    prefix: prefix specified in request as string
    marker: marker specified in request as string
    is_truncated: "true" if all objects in bucket were returned
    contents_list: list of GsListBucketResult.Contents instances
    common_prefixes: list of <CommonPrefixes>.<Prefix> names as strings
  """

  class Contents(object):
    """Contents class corresponding to like named element.

    Instance data:
      key: object name as string
      last_modified: time object last modified as string
      etag: object data etag value as string
      size: object size as string
      storage_class: object storage class as string
      owner_id: object owner google storage id as string
      owner_display_name: object owner name as string
    """

    def __init__(self):
      """Contents constructor."""
      self.key = ''
      self.last_modified = ''
      self.etag = ''
      self.size = ''
      self.storage_class = ''
      self.owner_id = ''
      self.owner_display_name = ''

  def __init__(self):
    """GsListBucketResult constructor."""
    self.name = ''
    self.prefix = ''
    self.marker = ''
    self.is_truncated = ''
    self.contents_list = []
    self.common_prefixes = []

  def parse_xml(self, xml_str):
    """Parses the given xml string to this object.

    Args:
      xml_str: ListBucketResult XML as string
    """
    self.contents_list = []
    self.common_prefixes = []
    dom = xml.dom.minidom.parseString(xml_str)
    self.name = self.value_from_elems(dom.getElementsByTagName('Name'))
    self.prefix = self.value_from_elems(dom.getElementsByTagName('Prefix'))
    self.marker = self.value_from_elems(dom.getElementsByTagName('Marker'))
    self.is_truncated = self.value_from_elems(
        dom.getElementsByTagName('IsTruncated'))
    contents_elems = dom.getElementsByTagName('Contents')
    for contents_elem in contents_elems:
      contents = GsListBucketResult.Contents()
      contents.key = self.value_from_elems(
          contents_elem.getElementsByTagName('Key'))
      contents.last_modified = self.value_from_elems(
          contents_elem.getElementsByTagName('LastModified'))
      contents.etag = self.value_from_elems(
          contents_elem.getElementsByTagName('ETag'))
      contents.size = self.value_from_elems(
          contents_elem.getElementsByTagName('Size'))
      contents.storage_class = self.value_from_elems(
          contents_elem.getElementsByTagName('StorageClass'))
      owner_elems = contents_elem.getElementsByTagName('Owner')
      for owner_elem in owner_elems:
        contents.owner_id = self.value_from_elems(
            owner_elem.getElementsByTagName('ID'))
        contents.owner_display_name = self.value_from_elems(
            owner_elem.getElementsByTagName('DisplayName'))
      self.contents_list.append(contents)
    common_prefixes_elems = dom.getElementsByTagName('CommonPrefixes')
    for common_prefixes_elem in common_prefixes_elems:
      prefix_elems = common_prefixes_elem.getElementsByTagName('Prefix')
      for prefix_elem in prefix_elems:
        self.common_prefixes.append(prefix_elem.childNodes[0].nodeValue)

  def get_keys(self):
    """Returns the list of object names found in self.contents_list."""
    return [c.key for c in self.contents_list]


class GsOperation(object):
  """Class to hold the important details of an HTTP request and response.

  Instance data:
    connection_host: host name connected to as string
    connection_port: host port connected to as int
    request_method: http request method ('GET', 'PUT', etc...) as string
    request_path_and_query: request URL path and query as string
    request_headers: request headers as dict
    response_status: response http status as int
    response_headers: response headers as dict
    response_error_body: response error body as string
  """

  def __init__(self):
    """GsOperation constructor."""
    self.connection_host = ''
    self.connection_port = 80
    self.request_method = ''
    self.request_path_and_query = ''
    self.request_headers = None
    self.response_status = 0
    self.response_headers = None
    self.response_error_body = None

  def __str__(self):
    """Convert instance to loggable string."""
    s = StringIO.StringIO()
    s.write('REQUEST:')
    s.write('\nSent to host: %s:%d' % (self.connection_host,
                                       self.connection_port))
    s.write('\n%s %s' % (self.request_method, self.request_path_and_query))
    if self.request_headers:
      for k, v in self.request_headers.iteritems():
        s.write('\n%s: %s' % (k, v))
    s.write('\nRESPONSE:')
    s.write('\n%d' % self.response_status)
    if self.response_headers:
      for k, v in self.response_headers.iteritems():
        s.write('\n%s: %s' % (k, v))
    if self.response_error_body:
      s.write('\n')
      s.write(self.response_error_body)
    return s.getvalue()


class GsClient(object):
  """Google Storage client.

  Instance data:
    access_key: google storage access key as string for authentication
    secret: google storage secret key as string for authentication
    host: google storage host as string
    proxy_host: optional proxy host
    proxy_port: optional proxy port
    auth_id: authentication type as string
    max_retries: max num retries for retryable errors
    max_redirects: max num redirects to follow
    operations: list of GsOperation instances for most recent request
      Note that each retry or redirection will append to this list.
    backoff_exponent: current backoff exponent during failures
  """

  def __init__(self,
               access_key=None,
               secret=None,
               host='commondatastorage.googleapis.com',
               proxy_host=None,
               proxy_port=80,
               auth_id='GOOG1',
               max_retries=5,
               max_redirects=10):
    """GsClient constructor.

    Args:
      access_key: google storage access key as string for authentication
      secret: google storage secret key as string for authentication
      host: google storage host as string
      proxy_host: optional proxy host
      proxy_port: optional proxy port
      auth_id: authentication type as string
      max_retries: max num retries for retryable errors
      max_redirects: max num redirects to follow
    """
    self.access_key = access_key
    self.secret = secret
    self.host = host
    self.proxy_host = proxy_host
    self.proxy_port = proxy_port
    self.auth_id = auth_id
    self.max_retries = max_retries
    self.max_redirects = max_redirects
    self.operations = []
    self.backoff_exponent = -1

  def get_service(self):
    """GET Service.

    Returns:
      GsListAllMyBucketsResult instance
    """
    outfile = StringIO.StringIO()
    self.send_request('GET', outfile=outfile)
    result = GsListAllMyBucketsResult()
    result.parse_xml(outfile.getvalue())
    return result

  def get_bucket(self,
                 bucket,
                 query_parameters=None):
    """GET Bucket.

    Args:
      bucket: bucket name as string
      query_parameters: query parameters as dict

    Returns:
      GsListBucketResult instance
    """
    outfile = StringIO.StringIO()
    self.send_request('GET',
                      bucket=bucket,
                      outfile=outfile,
                      query_parameters=query_parameters)
    result = GsListBucketResult()
    result.parse_xml(outfile.getvalue())
    return result

  def get_bucket_acl(self,
                     bucket):
    """GET Bucket ACL.

    Args:
      bucket: bucket name as string

    Returns:
      GsAccessControlList instance
    """
    outfile = StringIO.StringIO()
    self.send_request('GET',
                      bucket=bucket,
                      outfile=outfile,
                      query_parameters={'acl': None})
    acl = GsAccessControlList()
    acl.parse_xml(outfile.getvalue())
    return acl

  def get_object(self,
                 bucket,
                 key,
                 outfile,
                 extra_headers=None,
                 query_parameters=None,
                 chunk_size=0):
    """GET Object.

    Args:
      bucket: bucket name as string
      key: object name as string
      outfile: an open file-like object
        Only success responses will be written to this file.
        Error resonses will be found in the operation objects
      extra_headers: optional request headers as dict
      query_parameters: optional query parameters as dict
      chunk_size: size of each socket read (default of 0 = read all)
    """
    self.send_request('GET',
                      bucket=bucket,
                      key=key,
                      outfile=outfile,
                      extra_headers=extra_headers,
                      query_parameters=query_parameters,
                      chunk_size=chunk_size,
                      success_status_codes=GET_OBJECT_SUCCESS_CODES)

  def get_object_acl(self,
                     bucket,
                     key):
    """GET Object ACL.

    Args:
      bucket: bucket name as string
      key: object name as string

    Returns:
      GsAccessControlList instance
    """
    outfile = StringIO.StringIO()
    self.send_request('GET',
                      bucket=bucket,
                      key=key,
                      outfile=outfile,
                      query_parameters={'acl': None})
    acl = GsAccessControlList()
    acl.parse_xml(outfile.getvalue())
    return acl

  def head_object(self,
                  bucket,
                  key,
                  extra_headers=None):
    """HEAD Object.

    Args:
      bucket: bucket name as string
      key: object name as string
      extra_headers: optional request headers as dict

    Returns:
      response headers as dict
    """
    self.send_request('HEAD',
                      bucket=bucket,
                      key=key,
                      extra_headers=extra_headers)
    return self.operations[-1].response_headers

  def put_bucket(self,
                 bucket,
                 infile=None,
                 extra_headers=None,
                 query_parameters=None):
    """PUT Bucket.

    Args:
      bucket: bucket name as string
      infile: an open file-like object
        data in this file will be written to the http socket
      extra_headers: optional request headers as dict
      query_parameters: optional query parameters as dict
    """
    self.send_request('PUT',
                      bucket=bucket,
                      infile=infile,
                      extra_headers=extra_headers,
                      query_parameters=query_parameters)

  def put_bucket_acl(self,
                     bucket,
                     acl):
    """PUT Bucket ACL.

    Args:
      bucket: bucket name as string
      acl: GsAccessControlList instance
    """
    infile = StringIO.StringIO(acl.to_xml())
    self.put_bucket(bucket,
                    infile=infile,
                    query_parameters={'acl': None})

  def put_object(self,
                 bucket,
                 key,
                 infile,
                 extra_headers=None,
                 query_parameters=None,
                 chunk_size=0):
    """PUT Object.

    Args:
      bucket: bucket name as string
      key: object name as string
      infile: an open file-like object
        data in this file will be written to the http socket
      extra_headers: optional request headers as dict
      query_parameters: optional query parameters as dict
      chunk_size: size of each socket write (default of 0 = write all)
    """
    self.send_request('PUT',
                      bucket=bucket,
                      key=key,
                      infile=infile,
                      extra_headers=extra_headers,
                      query_parameters=query_parameters,
                      chunk_size=chunk_size)

  def put_object_acl(self,
                     bucket,
                     key,
                     acl):
    """PUT Object ACL.

    Args:
      bucket: bucket name as string
      key: object name as string
      acl: GsAccessControlList instance
    """
    infile = StringIO.StringIO(acl.to_xml())
    self.put_object(bucket,
                    key,
                    infile,
                    query_parameters={'acl': None})

  def delete_bucket(self,
                    bucket):
    """DELETE Bucket.

    Args:
      bucket: bucket name as string
    """
    self.send_request(
        'DELETE',
        bucket=bucket,
        success_status_codes=DEL_BUCKET_SUCCESS_CODES,
        retryable_status_codes=DEL_BUCKET_RETRYABLE_CODES)

  def delete_object(self,
                    bucket,
                    key):
    """DELETE Object.

    Args:
      bucket: bucket name as string
      key: object name as string
    """
    self.send_request('DELETE',
                      bucket=bucket,
                      key=key,
                      success_status_codes=DEL_OBJECT_SUCCESS_CODES)

  def send_request(self,
                   http_method,
                   bucket=None,
                   key=None,
                   infile=None,
                   outfile=None,
                   extra_headers=None,
                   query_parameters=None,
                   chunk_size=0,
                   success_status_codes=DEFAULT_SUCCESS_CODES,
                   retryable_status_codes=DEFAULT_RETRYABLE_CODES):
    """Sends the specifed request.

    Retries and follows redirection as necessary.

    Args:
      http_method: http method as string ('GET', 'PUT', etc...)
      bucket: bucket name as string
      key: object name as string
      infile: an open file-like object
        data in this file will be written to the http socket
      outfile: an open file-like object
        Only success responses will be written to this file.
        Error resonses will be found in the operation objects
      extra_headers: optional request headers as dict
      query_parameters: optional query parameters as dict
      chunk_size: size of each socket read/write (default of 0 = all)
      success_status_codes: response status codes considered success
      retryable_status_codes: response status codes considered retryable

    Returns:
      self.operations: the list of operations executed for this request.
    """
    self.operations = []
    operation = None
    redirect_location = None
    retries = 0
    redirects = 0
    while retries <= self.max_retries and redirects <= self.max_redirects:
      # Need backoff sleep?
      if self.backoff_exponent >= 0:
        self._backoff_sleep()
      # Prepare operation
      if redirect_location:
        operation = self._create_redirect_operation(
            operation, redirect_location)
        redirect_location = None
      else:
        operation = self._create_init_operation(
            http_method,
            bucket=bucket,
            key=key,
            extra_headers=extra_headers,
            query_parameters=query_parameters,
            infile=infile)
      # Execute operation
      try:
        operation = self._exec_operation(
            operation,
            infile=infile,
            outfile=outfile,
            chunk_size=chunk_size,
            success_status_codes=success_status_codes)
      except httplib.IncompleteRead, e:
        operation.response_error_body = (
            'IncompleteRead: %d bytes read' % (e.partial))
        retries += 1
        self._backoff_increment()
        continue
      finally:
        self.operations.append(operation)
      # Check for success
      if operation.response_status in success_status_codes:
        self._backoff_decrement()
        return self.operations
      # Check for redirect
      elif operation.response_status in REDIRECT_CODES:
        self._backoff_decrement()
        redirect_location = operation.response_headers['location']
        redirects += 1
        logging.debug('Redirected to %s', redirect_location)
        continue
      # Check for retryable failures
      elif operation.response_status in retryable_status_codes:
        self._backoff_increment()
        retries += 1
        continue
      else:
        self._backoff_increment()
        break
    raise GsError('Service Failure', self.operations)

  def _exec_operation(self,
                      operation,
                      infile=None,
                      outfile=None,
                      chunk_size=0,
                      success_status_codes=DEFAULT_SUCCESS_CODES):
    """Executes given operation request, and populates response."""
    connection = None
    try:
      logging.debug('%s %s %s',
                    operation.request_method,
                    operation.request_path_and_query,
                    str(operation.request_headers))
      # Connect
      connection = self._connect(operation.connection_host,
                                 operation.connection_port)
      # Write the first line of the request
      self._put_request(connection,
                        operation.request_method,
                        operation.request_path_and_query)
      # Write the headers
      self._put_headers(connection,
                        operation.request_headers)
      # Write the data
      if infile:
        self._write(connection, infile, chunk_size)
      else:
        # Flush the header write with no body
        connection.send('')
      # Get the response
      response = connection.getresponse()
      # Get the status
      operation.response_status = response.status
      # Read the response headers
      operation.response_headers = {}
      operation.response_headers.update(response.getheaders())
      # Read the response data (not for HEAD)
      if operation.request_method != 'HEAD':
        # Don't put data in outfile unless success status
        if operation.response_status in success_status_codes:
          if outfile:
            self._read(response, outfile, chunk_size)
        # Read the error body
        else:
          operation.response_error_body = response.read()
    finally:
      if connection:
        self._close(connection)
    return operation

  def _create_init_operation(self,
                             http_method,
                             bucket=None,
                             key=None,
                             extra_headers=None,
                             query_parameters=None,
                             infile=None):
    """Inits a new operation with request fields."""
    op = GsOperation()
    if self.proxy_host:
      op.connection_host = self.proxy_host
      op.connection_port = self.proxy_port
    else:
      op.connection_host = self.host
      op.connection_port = 80
    op.request_method = http_method
    path = self._get_path(bucket, key)
    query_string = self._get_query_string(query_parameters)
    op.request_path_and_query = path + query_string
    op.request_headers = self._get_request_headers(
        http_method,
        path,
        query_parameters,
        extra_headers,
        infile)
    return op

  def _create_redirect_operation(self,
                                 previous_operation,
                                 location):
    """Creates a new op based on the last op and the redirection."""
    parts = urlparse.urlparse(location)
    op = GsOperation()
    if self.proxy_host:
      op.connection_host = self.proxy_host
      op.connection_port = self.proxy_port
    else:
      host_and_port = parts.netloc.split(':')
      op.connection_host = host_and_port[0]
      if len(host_and_port) > 1:
        op.connection_port = int(host_and_port[1])
      else:
        op.connection_port = 80
    op.request_method = previous_operation.request_method
    op.request_path_and_query = parts.path
    if parts.query:
      op.request_path_and_query += '?%s' % parts.query
    op.request_headers = previous_operation.request_headers.copy()
    op.request_headers['Host'] = parts.netloc  # host and optional port
    return op

  def _backoff_decrement(self):
    """Decrements the backoff exponent toward min of -1 (off)."""
    if self.backoff_exponent > -1:
      self.backoff_exponent -= 1

  def _backoff_increment(self):
    """Increments the backoff exponent toward max of 5."""
    if self.backoff_exponent < 5:
      self.backoff_exponent += 1

  def _backoff_sleep(self):
    """Backoff sleep function called between retry attempts.

    See Google Storage docs for required exponential backoff
    when errors occur.
    Override this if you want it to do more.
    """
    sleep_sec = 1 << self.backoff_exponent
    logging.debug('Backoff sleep, retrying in %d seconds...', sleep_sec)
    time.sleep(sleep_sec)

  def _connect(self, host, port):
    """Returns a connection object.

    Override this if you have an alternate connection implementation.
    """
    return httplib.HTTPConnection(host, port=port)

  def _close(self, connection):
    """Closes the connection.

    Override this if you want it to do more.
    """
    connection.close()

  def _put_request(self,
                   connection,
                   http_method,
                   path_and_query):
    """Sends the method, path, and query to the connection.

    Override this if you want it to do more.
    """
    connection.putrequest(http_method,
                          path_and_query,
                          skip_host=True,
                          skip_accept_encoding=True)

  def _put_headers(self,
                   connection,
                   headers):
    """Sends the request headers to the connection.

    Override this if you want it to do more.
    """
    for name, val in headers.iteritems():
      connection.putheader(name, val)
    connection.endheaders()

  def _write(self, connection, infile, chunk_size):
    """Writes data in infile to the open connection.

    Override this if you want it to do more.
    Perhaps for performance measuring or periodic callbacks.
    """
    infile.seek(0)
    if chunk_size > 0:
      while True:
        chunk = infile.read(chunk_size)
        if chunk:
          connection.send(chunk)
        else:
          break
    else:
      connection.send(infile.read())

  def _read(self, response, outfile, chunk_size):
    """Reads data from response, and writes it to outfile.

    Override this if you want it to do more.
    Perhaps for performance measuring or periodic callbacks.
    """
    if chunk_size > 0:
      while True:
        chunk = response.read(chunk_size)
        if chunk:
          outfile.write(chunk)
        else:
          break
    else:
      outfile.write(response.read())
    outfile.flush()

  def _get_request_headers(self,
                           http_method,
                           path,
                           query_parameters,
                           extra_headers,
                           infile):
    """Returns the request header dict based on args."""
    headers = {}
    # Content-Length
    if infile:
      infile.seek(0, os.SEEK_END)
      headers['Content-Length'] = infile.tell()
    else:
      headers['Content-Length'] = '0'
    # Date
    headers['Date'] = time.strftime('%a, %d %b %Y %H:%M:%S GMT',
                                    time.gmtime())
    # Host
    headers['Host'] = self.host
    # User-Agent
    headers['User-Agent'] = 'gslite/' + __version__
    # Add extra headers
    if extra_headers:
      headers.update(extra_headers)
    # Authorization
    if self.access_key and self.secret:
      headers['Authorization'] = self._get_authentication(
          http_method,
          path,
          query_parameters,
          headers)
    return headers

  def _get_path(self, bucket, key):
    """Returns the URL path based on args."""
    s = StringIO.StringIO()
    s.write('/')
    if bucket:
      s.write(urllib.quote(bucket))
      if key:
        s.write('/')
        s.write(urllib.quote(key))
    return s.getvalue()

  def _get_query_string(self, query_parameters):
    """Returns the URL query string based on query dict."""
    s = StringIO.StringIO()
    if query_parameters:
      s.write('?')
      first = True
      for name, val in query_parameters.iteritems():
        if first:
          first = False
        else:
          s.write('&')
        s.write(name)
        if val:
          s.write('=%s' % urllib.quote(str(val)))
    return s.getvalue()

  def _get_authentication(self, http_method, path, query_parameters, headers):
    """Returns the Authorization header value based on args."""
    string_to_sign = StringIO.StringIO()
    # HTTP method
    string_to_sign.write('%s\n' % http_method)
    # Content-Md5
    if 'Content-MD5' in headers:
      string_to_sign.write(headers['Content-MD5'].strip())
    string_to_sign.write('\n')
    # Content-Type
    if 'Content-Type' in headers:
      string_to_sign.write(headers['Content-Type'].strip())
    string_to_sign.write('\n')
    # Date
    if ('x-goog-date' not in headers and
        'Date' in headers):
      string_to_sign.write(headers['Date'])
    string_to_sign.write('\n')
    # Extension headers
    sorted_header_keys = headers.keys()
    sorted_header_keys.sort()
    for header_key in sorted_header_keys:
      if header_key.startswith('x-goog-'):
        string_to_sign.write('%s:%s\n' % (
            header_key, headers[header_key]))
    # Resource
    string_to_sign.write(path)
    if query_parameters:
      for subresource in ('acl', 'location', 'logging', 'torrent'):
        if subresource in query_parameters:
          string_to_sign.write('?%s' % subresource)
          # should only be one of these
          break
    # HMAC-SHA1
    h = hmac.new(self.secret, digestmod=hashlib.sha1)
    h.update(string_to_sign.getvalue())
    signature = base64.b64encode(h.digest())
    # Put it all together
    return '%s %s:%s' % (self.auth_id, self.access_key, signature)
