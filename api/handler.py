#!/usr/bin/env python
#
# Copyright 2011 Google Inc.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""Configures the API handler for the application."""



from datetime import datetime
import logging

from google.appengine.dist import use_library
use_library('django', '1.2')

from django.utils import simplejson
from google.appengine.ext import db
from google.appengine.ext import webapp
from iso8601 import parse_date
from google.appengine.ext.webapp.util import run_wsgi_app
import model


class IncidentError(Exception):
  """Base class for any errors handling Incidents."""
  pass


class InvalidIncidentIdError(IncidentError):
  """Raised if a request with an invalid Incident ID is received."""
  pass


class InvalidListError(IncidentError):
  """Raised if a request is received with invalid list query paremeters."""
  pass


class InvalidDateError(IncidentError):
  """Raised if a request is received with invalid date query paremeters."""
  pass


class IncidentNotFoundError(IncidentError):
  """Raised if an Incident for the given ID cannot be found."""
  pass


class IncidentHandler(webapp.RequestHandler):
  """Handles all RESTful operations on Incidents."""

  def _GetIdFromUri(self):
    """Return the ID from the current request URI, or None if no ID found."""
    parts = self.request.path.split('/')
    if len(parts) == 5 and parts[-1]:
      try:
        return int(parts[-1])
      except ValueError:
        raise InvalidIncidentIdError()
    return None

  def _HandleInvalidIncidentIdError(self):
    """Handle an InvalidIncidentIdError, yield an HTTP 400."""
    self.response.clear()
    self.response.set_status(400)
    self.response.out.write('The incident ID you provided is not an integer.')

  def _HandleInvalidListError(self):
    """Handle an InvalidListError, yield an HTTP 400."""
    self.response.clear()
    self.response.set_status(400)
    self.response.out.write(
        'One of the list parameters in your request was not in CSV format.')

  def _HandleInvalidDateError(self):
    """Handle an InvalidIncidentIdError, yield an HTTP 400."""
    self.response.clear()
    self.response.set_status(400)
    self.response.out.write(('One of the date parameters in your request was '
                             'not in ISO 8601 format.'))

  def _HandleIncidentNotFoundError(self):
    """Handle an IncidentNotFoundError, yield an HTTP 404."""
    self.response.clear()
    self.response.set_status(404)
    self.response.out.write(
        'An incident with the provided ID could not be found.')

  def _HandleUnknownError(self, log_message):
    """Handle an unknown error, yield an HTTP 500, and log the given message.

    Args:
      log_message: Message giving details about what happened.
    """
    logging.error(log_message)
    self.response.clear()
    self.response.set_status(500)
    self.response.out.write(
        'An unknown error has occurred. Please try your request again.')

  def _GetIncidentByUriId(self):
    """Try to find an Incident based on the ID in this request's URI.

    Returns:
      Found Incident, or None if no Incident was found.
    Raises:
      IncidentNotFoundError: Incident with given ID not found in datastore.
    """
    incident_id = self._GetIdFromUri()
    incident = model.Incident.get_by_id(incident_id)
    if incident is None:
      raise IncidentNotFoundError()
    return incident

  def get(self):
    """Retrieve either all incidents, or a single incident by ID.

    If an ID is found in the current request URI, then look up that individual
    Incident, and render it as output.

    If no ID is found in the current request URI, render all Incidents as
    output.
    """
    try:
      incident_id = self._GetIdFromUri()
      if incident_id is not None:
        self._GetById()
      else:
        self._GetAll()
    except InvalidIncidentIdError:
      self._HandleInvalidIncidentIdError()

  def _GetById(self):
    """Render the Incident with the given ID to output."""
    try:
      incident = self._GetIncidentByUriId()
      self.response.out.write(simplejson.dumps(
          incident.GetDict(), default=IncidentHandler._DateToSerializable))
    except IncidentNotFoundError:
      self._HandleIncidentNotFoundError()
    except db.Error, e:
      self._HandleUnknownError(e)

  @staticmethod
  def _DateToSerializable(obj):
    """simplejson can't render dates.  Helper method to fix that."""
    if isinstance(obj, datetime):
      return obj.isoformat()

  @staticmethod
  def _StrToDatetime(value):
    """Convert the given ISO formatted string into a datetime."""
    return parse_date(value)

  def _ApplyRequestFiltersToQuery(self, query):
    """Apply query parameters as query filters."""
    # Only one field can be filtered with an inequality operation at a time.
    inequality_field = None
    for param, property_operator, func, inequality in INCIDENT_FILTERS:
      if self.request.get(param) and (not inequality_field or
                                      not inequality or
                                      inequality_field == inequality):
        func(query, property_operator, self.request.get(param))
        inequality_field = inequality_field or inequality

    return query

  def _GetAll(self):
    """Render all Incidents to output."""
    try:
      incidents = self._ApplyRequestFiltersToQuery(model.Incident.all())
      self.response.out.write(simplejson.dumps(
          map(lambda i: i.GetDict(), incidents),
          default=IncidentHandler._DateToSerializable))
    except InvalidListError:
      self._HandleInvalidListError()
    except InvalidDateError:
      self._HandleInvalidDateError()
    except db.Error, e:
      self._HandleUnknownError(e)

  def post(self):
    """Create the Incident described by the request POST body."""
    try:
      incident = model.Incident.FromJson(self.request.body)
      incident.id = None
      incident.put()
      model.Tag.CreateMissingTags(incident)
      self.response.set_status(201)
    except db.Error, e:
      self._HandleUnknownError(e)

  def put(self):
    """Update the Incident described by the request body."""
    try:
      incident = self._GetIncidentByUriId()
      new_incident = model.Incident.FromJson(self.request.body)
      incident.Merge(new_incident)
      incident.put()
      incident.PurgeJsonCache()
      model.Tag.CreateMissingTags(incident)
      self.response.set_status(204)
    except IncidentNotFoundError:
      self._HandleIncidentNotFoundError()
    except db.Error, e:
      self._HandleUnknownError(e)

  def delete(self):
    """Delete the Incident with the given ID."""
    try:
      incident = self._GetIncidentByUriId()
      incident.delete()
      self.response.set_status(204)
    except IncidentNotFoundError:
      self._HandleIncidentNotFoundError()
    except db.Error, e:
      self._HandleUnknownError(e)

  @staticmethod
  def ApplyFilter(query, property_operator, value):
    """Apply a single filter to the model query.

    Args:
      query: App Engine model query to apply the filters to.
      property_operator: String containing the property name, and an optional
                         comparison operator.
      value: Single filter.
    Returns:
      Query.
    """
    query.filter(property_operator, value)
    return query

  @staticmethod
  def ApplyListFilter(query, property_operator, filters):
    """Apply a list of filter to the model query.

    Args:
      query: App Engine model query to apply the filters to.
      property_operator: String containing the property name, and an optional
                         comparison operator.
      filters: Comma seperated string.
    Returns:
      Query.
    Raises:
      InvalidListError.
    """
    try:
      for tag in filters.split(','):
        if tag:
          query.filter(property_operator, tag)
    except ValueError:
      raise InvalidListError
    return query

  @staticmethod
  def ApplyDateFilter(query, property_operator, date):
    """Apply a date filter to the model query.

    Args:
      query: App Engine model query to apply the filters to.
      property_operator: String containing the property name, and an optional
                         comparison operator.
      date: String representing a date value.
    Returns:
      Query.
    Raises:
      InvalidListError.
    """
    try:
      query.filter(property_operator, parse_date(date))
    except TypeError:
      raise InvalidDateError
    except ValueError:
      raise InvalidDateError
    return query


# Incident filters to be used in IncidentHandler._ApplyRequestFiltersToQuery
# method and in landing.LandingPage to display the filters.
# List of tuples consisting of:
#   * Query parameter.
#   * Property operator on the model query.
#   * Function to apply the filter to the model query (e.g _ApplyFilter).
#   * The property name on which an inequality operation is applied. None if
#     operation is not an inequality operation.
INCIDENT_FILTERS = [
    ('accepted_tags', 'accepted_tags = ', IncidentHandler.ApplyListFilter,
     None),
    ('suggested_tags', 'suggested_tags = ', IncidentHandler.ApplyListFilter,
     None),
    ('owner', 'owner = ', IncidentHandler.ApplyFilter, None),
    ('created_before', 'created < ', IncidentHandler.ApplyDateFilter,
     'created'),
    ('created_after', 'created > ', IncidentHandler.ApplyDateFilter, 'created'),
    ('updated_before', 'updated < ', IncidentHandler.ApplyDateFilter,
     'updated'),
    ('updated_after', 'updated > ', IncidentHandler.ApplyDateFilter, 'updated'),
    ('resolved_before', 'resolved < ', IncidentHandler.ApplyDateFilter,
     'resolved'),
    ('resolved_after', 'resolved > ', IncidentHandler.ApplyDateFilter,
     'resolved'),
]


def main():
  """Runs the application."""
  application = webapp.WSGIApplication(
      [('/resources/v1/incidents.*', IncidentHandler)],
      debug=True)
  run_wsgi_app(application)


if __name__ == '__main__':
  main()