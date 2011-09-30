// Copyright 2011 Google Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


/**
 * @fileoverview
 * Provides methods for the autodo web client, for making
 * calls to the autodo API, as well as rendering
 * and bindings for UI navigation elements.
 *
 */

/** google global namespace for Google projects. */
var google = google || {};

/** devrel namespace for Google Developer Relations projects. */
google.devrel = google.devrel || {};

/** samples namespace for Devrel sample code. */
google.devrel.samples = google.devrel.samples || {};

/** autodo namespace for this sample. */
google.devrel.samples.autodo = google.devrel.samples.autodo || {};

/** autodo alias for the google.devrel.samples.autodo namespace. */
var autodo = google.devrel.samples.autodo;

/** ApiClient object for API methods. */
google.devrel.samples.autodo.ApiClient =
    google.devrel.samples.autodo.ApiClient || {};

/** Bindings object for binding actions to UI elements. */
google.devrel.samples.autodo.Bindings =
    google.devrel.samples.autodo.Bindings || {};

/** Data object for persisting server data on the client. */
google.devrel.samples.autodo.Data =
    google.devrel.samples.autodo.Data || {};

/** Render object for rendering content. */
google.devrel.samples.autodo.Render =
    google.devrel.samples.autodo.Render || {};

/** Util object for autodo utility methods. */
google.devrel.samples.autodo.Util =
    google.devrel.samples.autodo.Util || {};

/** Aliases for autodo objects. */
var ApiClient = google.devrel.samples.autodo.ApiClient;
var Bindings = google.devrel.samples.autodo.Bindings;
var Data = google.devrel.samples.autodo.Data;
var Render = google.devrel.samples.autodo.Render;
var Util = google.devrel.samples.autodo.Util;

/**
 * Base URI of the autodo API.
 * @type {string}
 */
google.devrel.samples.autodo.ApiClient.BASE_URI = '/resources/v1/incidents/';

/**
 * Default search options for list view.
 * @type {string}
 */
google.devrel.samples.autodo.ApiClient.DEFAULT_SEARCH = '';

/**
 * The last parameters used to call the autodo API.
 * meant for use with nav elements (i.e."back" button).
 * @type {string}
 */
google.devrel.samples.autodo.ApiClient.currentListView =
    ApiClient.DEFAULT_SEARCH;

/**
 * Displays the list of incidents, represented by the currentListView. If a
 * current copy of the list exists in cache, show that. If not, call the API.
 * @param {Object=} opt_options parameter list to pass to API.
 */
google.devrel.samples.autodo.ApiClient.listView = function(opt_options) {
  Render.showInfoMessage('Loading Incident List...');
  var opt_options = opt_options || ApiClient.DEFAULT_SEARCH;
  var uri = ApiClient.BASE_URI;
  if (opt_options) {
    uri += '?' + opt_options;
  }
  ApiClient.currentListView = opt_options;

  if (ApiClient.useIncidentCache) {
    $.ajax({
      url: uri,
      type: 'GET',
      dataType: 'json',
      success: [Render.incidentTable, Data.persistIncidentList]
    });
  } else {
    Render.incidentTable(Data.incidentList);
  }
};

/**
 * Returns data from an API request for info about a single incident.
 * @param {string} id The id of a single incident.
 * @param {string} callback The callback function to send data to upon success.
 * @param {Object=} opt_params Optional parameters to pass to callback function.
 */
google.devrel.samples.autodo.ApiClient.getIncidentData = function(
    id, callback, opt_params) {
  var incidentData;
  $.ajax({
    url: ApiClient.BASE_URI + id,
    type: 'GET',
    dataType: 'json',
    success: function(data) {
      callback(data, opt_params);
    }
  });
};

/**
 * Calls the autodo API for a particular incident.
 * @param {Object} data Data representation of an incident.
 */
google.devrel.samples.autodo.ApiClient.incidentView = function(data) {
  Render.showInfoMessage('Loading Incident...');
  Render.singleIncident(data);
};

/**
 * Calls the autodo API with the input search terms.
 */
google.devrel.samples.autodo.ApiClient.search = function() {
  var raw_query = $('input#search-box').val();

  if (raw_query) {
    var queries = [];
    var tokens = raw_query.split(Util.regex);

    for (var i = 0; i < tokens.length; ++i) {
      var token = tokens[i];

      if (Util.regex.test(token)) {
        if (++i < tokens.length) {
          queries.push(token + escape(tokens[i].trim()));
        }
      }
    }

    ApiClient.listView(queries.join('&'));
  }
};

/**
 * Calls the autodo API to assign an incident to a new owner.
 * @param {Object} data Data representation of an incident.
 * @param {Object} owner An object containing name of new incident owner.
 */
google.devrel.samples.autodo.ApiClient.assignIncident = function(data, owner) {
  Render.showInfoMessage('Assigning incident...');
  data.owner = owner.name;
  $.ajax({
    url: ApiClient.BASE_URI + data.id,
    type: 'PUT',
    data: JSON.stringify(data),
    contentType: 'application/json',
    success: function(data) {
      // Reset incident cache.
      Data.incidentListUpdated = 0;
      ApiClient.listView(ApiClient.currentListView);
    }
  });
};

/**
 * Iterates through and updates a list of incidents with associated tags.
 * @param {Object} incidentTagList Object of ids with accepted and
 * suggested tags.
 */
google.devrel.samples.autodo.ApiClient.updateTags = function(
    incidentTagList) {
  Render.showInfoMessage('Updating labels...');
  $.each(incidentTagList, function(id, tagsList) {
    ApiClient.getIncidentData(id,
                              ApiClient.updateIncidentTags,
                              {'id': id, 'tagsList': tagsList});
  });
};

/**
 * Calls the autodo API to add accepted tags to an incident.
 * @param {Object} data Data representation of an incident.
 * @param {Object} incident An object containing updated incident tags.
 */
google.devrel.samples.autodo.ApiClient.updateIncidentTags = function(
    data, incident) {
  data.accepted_tags = incident.tagsList.accepted_tags;
  data.suggested_tags = incident.tagsList.suggested_tags;
    $.ajax({
      url: ApiClient.BASE_URI + incident.id,
      type: 'PUT',
      data: JSON.stringify(data),
      contentType: 'application/json',
      success: function(data) {
        // Reset incident cache.
        Data.incidentListUpdated = 0;
        Render.updateIncidentListTags(incident.id, incident.tagsList);
      }
    });
};

/**
 * Indicates if ApiClient should use the data cache for storing incident list.
 * @return {boolean} Whether to use the cache (true) or not.
 */
google.devrel.samples.autodo.ApiClient.useIncidentCache = function() {
  now = new Date().getTime();
  delta = now - Data.incidentListUpdated;
  shouldUseIncidentCache = delta > Data.INCIDENT_CACHE_LENGTH * 1000 ||
      ApiClient.currentListView != Data.incidentListType ||
      !Data.USE_INCIDENT_CACHE;
  return shouldUseIncidentCache;
};

/**
 * Indicates how old the incident cache may be before it needs to be refreshed
 * (in seconds).
 * @type {number}
 */
google.devrel.samples.autodo.Data.INCIDENT_CACHE_LENGTH = 60;

/**
 * Whether or not to use the local incident cache.
 * @type {boolean}
 */
google.devrel.samples.autodo.Data.USE_INCIDENT_CACHE = true;

/**
 * Indicates the type of list view that is represented by the cached incident
 * list.
 * @type {string}
 */
google.devrel.samples.autodo.Data.incidentListType = '';

/**
 * Indicates when the incident list was last updated.
 * @type {number}
 */
google.devrel.samples.autodo.Data.incidentListUpdated = 0;

/**
 * Persists a list of incidents from the server in a local object.
 * @param {Object} incidentList List of incidents retrieved from the server.
 */
google.devrel.samples.autodo.Data.persistIncidentList = function(
    incidentList) {
  Data.incidentList = incidentList;
  Data.incidentListType = ApiClient.currentListView;
  Data.incidentListUpdated = new Date().getTime();
};

/**
 * Renders a message during loading actions.
 * @param {string} message Message to be shown during action.
 */
google.devrel.samples.autodo.Render.showInfoMessage = function(message) {
  $('#notificationbar').find('.message').text(message);
};

/**
 * Replaces the toggle checkbox with a back button.
 */
google.devrel.samples.autodo.Render.backButton = function() {
  $('a.select-button').replaceWith(
    '<a class="back-button atd-button no-margin"' +
    'data-tooltip="Back to Case List">' +
    '<img src="/images/theme/icons/back.gif" /></a>'
  );
  $('a.back-button').click(function() {
    ApiClient.listView(ApiClient.currentListView);
  });
};

/**
 * Renders a checkbox element based on optional parameters.
 * @param {Object=} opt_properties Object optional checkbox element properties.
 * @return {Object} A checkbox jQuery element.
 */
google.devrel.samples.autodo.Render.checkBox = function(opt_properties) {
  var checkBox = $('<input>').attr({
                   'type': 'checkbox',
                   'class': opt_properties.cssClass,
                   'id': opt_properties.id,
                   'value': opt_properties.value});
  return checkBox;
};

/**
 * Renders an incident view from API call data.
 * @param {Object} data JSON object containing incident list info.
 */
google.devrel.samples.autodo.Render.incidentTable = function(data) {
  if ($('a.back-button').length) {
    $('a.back-button').replaceWith(
    '<a class="select-button atd-button no-margin" data-tooltip="Select">' +
    '<input type="checkbox" id="toggle-all">' +
    '</a>'
    );
  }
  // Array to hold accumulated global tag list.
  var tagList = Array();
  $('#content').empty();
  $('#content').append($('<table>').addClass('list'));
  $.each(data, function(i, incident) {
    var trow = $('<tr>');
    var checkBox = Render.checkBox({
      cssClass: 'content_checkbox', value: incident.id
    });
    trow.append($('<td>').append(checkBox));
    // Incident title and message summary.
    var incidentTd = $('<td>');
    var incidentDiv = $('<div>').addClass(
        'content-list-div').attr('value', incident.id);
    // <p> tags are used as throwaway tags for HTML sanitization and don't
    // appear in the UI.
    incidentDiv.append($('<strong>').append($('<p>').text(
        incident.title + ' - ').html()));
    incidentDiv.append($('<p>').text(incident.messages[0].body).html());
    Render.tagList(incidentTd, incident);
    trow.append(incidentTd.append(incidentDiv));
    // Incident timestamp.
    trow.append($('<td>').append(
        google.devrel.samples.autodo.Util.formatDateStamp(incident.created))
    );
    $('#content > table').append(trow);
    // Add current incident tags to global list if not duplicates.
    var allTags = incident.accepted_tags.concat(incident.suggested_tags);
    for (var i = 0; i < allTags.length; i++) {
      if ($.inArray(allTags[i], tagList) == -1) {
        tagList.push(allTags[i]);
      }
    }
  });
  Render.reloadButton();
  Render.popUpCheckboxes(tagList);
  Bindings.bindIncidentLink();
  Bindings.bindCheckBoxes();
  Bindings.bindAcceptTags();
  Bindings.bindRemoveTags();
};

/**
 * Renders a button to reload an incident list.
 */
google.devrel.samples.autodo.Render.reloadButton = function() {
  if ($('a.original-button').length) {
    $('a.original-button').replaceWith(
    '<a class="reload-button atd-button" data-tooltip="Reload">' +
    '<img src="/images/theme/icons/reload.png" /></a>'
    );
  }
  google.devrel.samples.autodo.Bindings.bindReloadButton();
};

/**
 * Renders a button linking to the original case.
 * @param {string} canonicalLink of the original incident.
 */
google.devrel.samples.autodo.Render.originalButton = function(canonicalLink) {
  if ($('a.reload-button').length) {
    $('a.reload-button').replaceWith(
    '<a class="original-button atd-button" data-tooltip="Original">' +
    'Original</a>'
    );
  }
  google.devrel.samples.autodo.Bindings.bindOriginalButton(canonicalLink);
};

/**
 * Returns a list of all checked tags in the options menu.
 * @return {Object} List of checked tags in options menu.
 */
google.devrel.samples.autodo.Bindings.findCheckedTagOptions = function() {
  var optionsTagsList = new Array();
  $('#label-options').find('input:checked').each(function() {
    optionsTagsList.push($(this).val());
  });
  if ($('#label-options').find('input[type="text"]').val().length) {
    optionsTagsList.push(
      $('#label-options').find('input[type="text"]').val()
    );
  }
  return optionsTagsList;
};

/**
 * Returns a list of accepted and suggested tags from an incident.
 * @param {Object} incidentElement An element object.
 * @return {Object} a List of suggested and accepted tags for an incident.
 */
google.devrel.samples.autodo.Bindings.findIncidentTags = function(
    incidentElement) {
  var incidentTags = {};
  optionsTagsList = Bindings.findCheckedTagOptions();
  incidentTags.id = parseInt(incidentElement.attr('value'));
  incidentTags.accepted_tags = new Array();
  for (var i = 0; i < optionsTagsList.length; i++) {
    incidentTags.accepted_tags.push(optionsTagsList[i]);
  }
  incidentTags.suggested_tags = [];
  incidentElement.parentsUntil('tr').parent().find('.label').each(function() {
    if (incidentElement.hasClass('accepted')) {
      if (jQuery.inArray(incidentElement.attr('value'),
                         incidentTags.accepted_tags) == -1) {
        incidentTags.accepted_tags.push(incidentElement.attr('value'));
      }
    } else if (incidentElement.hasClass('suggested')) {
      incidentTags.suggested_tags.push(incidentElement.attr('value'));
    }
  });
  return incidentTags;
};

/**
 * Returns a list of elements and ids corresponding to incidents that need
 * to be updated.
 * @param {Object} contentElement Div element to parse for updated incidents.
 * @return {Object} Object containing elements and corresponding element id
 * for each incident to update.
 */
google.devrel.samples.autodo.Bindings.findIncidentUpdates = function(
    contentElement) {
  var incidentTagList = {};
  var optionsTagsList = Bindings.findCheckedTagOptions();
  contentElement.find('input:checked, div.title.content-list-div').each(
      function() {
    if ($(this).length && optionsTagsList.length) {
      var incidentTags = Bindings.findIncidentTags($(this));
        incidentTagList[$(this).attr('value')] = incidentTags;
      }
  });
  return incidentTagList;
};

/**
 * Renders a list of tags.
 * @param {Object} incidentTd td element to append tags to.
 * @param {Object} incident An Object containing tag data.
 */
google.devrel.samples.autodo.Render.tagList = function(incidentTd, incident) {
  $.each(incident.accepted_tags.sort(), function(index, tag) {
    incidentTd.prepend($('<span>').addClass('label accepted').
        attr('value', tag).append(tag));
  });
  $.each(incident.suggested_tags.sort(), function(index, tag) {
    incidentTd.prepend($('<span>').addClass('label suggested').
        attr('value', tag).append(tag));
  });
  incidentTd.find('span.suggested').append($('<a>').addClass('accept').attr(
      'href', '#').html('&#x2713;'));
  incidentTd.find('span.label').append($('<a>').addClass('remove').attr(
      'href', '#').html('&#x2717'));
};

/**
 * Updates display of the tags for a incident in a list view.
 * @param {string} id The id of the incident.
 * @param {Object} tagsList tags associated with incident.
 */
google.devrel.samples.autodo.Render.updateIncidentListTags = function(
    id, tagsList) {
  var tagsTd = $('.content-list-div[value="' + id + '"]').parent();
  tagsTd.find('.label').remove();
  Render.tagList(tagsTd, tagsList);
  Bindings.bindAcceptTags();
  Bindings.bindRemoveTags();
};

/**
 * Position pop-up options boxes.
 */
google.devrel.samples.autodo.Render.popUpPosition = function() {
  var assignPosition = $('#assign-button').offset();
  var labelPosition = $('#label-button').offset();
  $('#assign-options').offset({
    top: assignPosition.top + $('#assign-button').outerHeight(),
    left: assignPosition.left
  });
  $('#label-options').offset({
    top: labelPosition.top + $('#label-button').outerHeight(),
    left: labelPosition.left
  });
  google.devrel.samples.autodo.Bindings.blurPopUpMenus();
};

/**
 * Populates tag popup checkboxes with current tag values.
 * @param {Array} tagList List of tags to populate.
 */
google.devrel.samples.autodo.Render.popUpCheckboxes = function(
    tagList) {
  tagList.sort();
  $('#label-options').find('.options-checkboxes').empty();
  for (var i = 0; i < tagList.length; i++) {
    var checkBox = Render.checkBox({
      id: 'tag_' + tagList[i], value: tagList[i]
    });
    $('#label-options').find('.options-checkboxes').append(checkBox);
    var checkboxLabel = $('<label>').after('<br />');
    checkboxLabel.text(tagList[i]).attr({for: 'tag_' + tagList[i]});
    $('#label-options').find('.options-checkboxes').append(checkboxLabel);
  }
};

/**
 * Renders a single incident view from API call data.
 * @param {Object} data JSON object containing incident list info.
 */
google.devrel.samples.autodo.Render.singleIncident = function(data) {
  Render.backButton();
  Render.originalButton(data.canonical_link);
  $('#content').empty();
  incidentTd = $('<td>').css('padding', '10px 0px 10px 0px');
  // <p> tags are used as throwaway tags for HTML sanitization and don't
  // appear in the UI.
  var titleText = $('<p>').text(data.title).html();
  var titleDiv = $('<div>').addClass('title').append(titleText);
  titleDiv.addClass('content-list-div').attr('value', data.id);
  Render.tagList(incidentTd, data);
  $('titleDiv .label').append($('<a>'));
  incidentTd.append(titleDiv);
  $('#content').append($('<table>').addClass('incident'));
  $('#content > table').append($('<tr>').append(incidentTd));
  $.each(data.messages, function(i, message) {
    var td = $('<td>');
    td.append($('<div>').addClass('incident-date').append(
        Util.formatDateStamp(message.sent)));
    // <p> tags are used as throwaway tags for HTML sanitization and don't
    // appear in the UI.
    var authorText = $('<p>').text(message.author + ': ').html();
    td.append($('<div>').addClass('incident').append(authorText));
    var messageText = $('<p>').text(message.body).html().replace(
        /\r?\n/g, '<br />');
    td.append($('<div>').addClass('incident').append(messageText));
    $('#content > table').append($('<tr>').append(td));
  });
  Bindings.bindAcceptTags();
  Bindings.bindRemoveTags();
};

/**
 * Renders a list of application settings.
 */
google.devrel.samples.autodo.Render.settingsList = function() {
  Render.backButton();
  Render.showInfoMessage('Loading Settings...');
  $.ajax({
    url: '/settings',
    type: 'GET',
    dataType: 'html',
    success: function(data) {
        $('#content').empty();
        $('#content').append(data);
    }
  });
};

/**
 * Sets a sidebar <li> element to "selected".
 * @param {Object} li JQuery sidebar li object to be toggled.
 */
google.devrel.samples.autodo.Render.selectedSidebarLink = function(li) {
  $('#sidebar li').removeClass('selected');
  $(li).addClass('selected');
};

/**
 * Binds incident checkboxes to the master checkbox toggle.
 */
google.devrel.samples.autodo.Bindings.bindCheckBoxes = function() {
  $('#toggle-all').change(function() {
    var checkedStatus = this.checked;
    $('.content_checkbox').each(function() {
      this.checked = checkedStatus;
    });
  });
};

/**
 * Binds the popUp menu tag input box to create new tag action.
 */
google.devrel.samples.autodo.Bindings.bindTagTextInput = function() {
  var tagList = new Array();
  var inputBox = $('#label-options').find('input[type="text"]');

  $('#label-options').find('input:checked').each(function() {
    tagList.push($(this).val());
  });
  inputBox.keyup(function() {
    var validTag = /^\w+(\-\w+)?$/;
    var spaceChar = /\s/;
    var badChar = /\W/;
    var tooMany = /\-\w*\-/;
    var needMore = /\-$/;
    var badStart = /^\-/;
    var userEntered = $(this).val();

    $('.error').hide();

    if (inputBox.val().length && !validTag.test(userEntered)) {
      var errorMessage = 'Tag format=\"model-Tag_Name\".';

      $('#apply-tags').hide();

      if (badStart.test(userEntered)) {
        errorMessage = 'Cannot start with a hyphen.';
      } else if (needMore.test(userEntered)) {
        errorMessage = 'Cannot end with a hyphen. Please add more.';
      } else if (tooMany.test(userEntered)) {
        errorMessage = 'Only one hyphen allowed.<br/>e.g. \"model-Tag_Name\"';
      } else if (spaceChar.test(userEntered)) {
        errorMessage = 'Cannot use spaces. Use _ instead.';
      } else if (badChar.test(userEntered)) {
        errorMessage = 'Invalid character.<br/>Use A-Za-z0-9 and -.';
      }

      inputBox.after('<span class="error"><br/>' + errorMessage + '</span>');
    } else {
      $('#apply-tags').show();

      $('#apply-tags').text('"' + $(this).val() + '" (create new)');
      if (userEntered.length < 1) {
        $('#apply-tags').text('Apply');
      }
    }
  });
  Render.popUpCheckboxes(tagList);
};

/**
 * Binds the "apply" button to a tag update.
 */
google.devrel.samples.autodo.Bindings.bindTagOptions = function() {
  $('#apply-tags').click(function() {
    var incidentTagList = Bindings.findIncidentUpdates($('#content'));
    $('#label-options').hide();
    $('#label-options').find('input[type="text"]').val('');
    $('#apply-tags').text('Apply');
    ApiClient.updateTags(incidentTagList);
  });
};

/**
 * Binds the "apply" button to ownership assignment.
 */
google.devrel.samples.autodo.Bindings.bindAssignOptions = function() {
  $('#assign-owner').click(function() {
    // Get the name of the new owner.
    var owner = '';
    var textValue = $('#assign-options').find('input[type="text"]').val();
    if (textValue.length) {
      owner = textValue;
    }
    // Retrieve incidents that should be updated.
    $('#content').find('input:checked, div.title.content-list-div').each(
        function() {
      ApiClient.getIncidentData($(this).attr('value'),
                                ApiClient.assignIncident,
                                {'name': owner});
    });
    $('#assign-options').hide();
    $('#assign-options').find('input[type="text"]').val('');
  });
}

/**
 * Binds incidents list elements to render incident views.
 */
google.devrel.samples.autodo.Bindings.bindIncidentLink = function() {
  $('div.content-list-div').click(function() {
    var id = $(this).attr('value');
    ApiClient.getIncidentData(id, ApiClient.incidentView);
  });
};

/**
 * Binds sidebar <li> elements to render incident lists.
 */
google.devrel.samples.autodo.Bindings.bindSideBar = function() {
  $('#sidebar').find('li').click(function() {
    Render.selectedSidebarLink($(this));
  });
  $('li.mine').click(function() {
    ApiClient.listView('owner=' + currentUser);
  });
  $('li.unassigned').click(function() {
    ApiClient.listView('owner=none');
  });
  $('li.resolved').click(function() {
    ApiClient.listView();
  });
  $('li.all').click(function() {
    ApiClient.listView('owner=');
  });
};

/**
 * Binds search box and search button elements.
 */
google.devrel.samples.autodo.Bindings.bindSearchInputs = function() {
  var searchBox = $('input#search-box');

  searchBox.autocomplete({
      source: function(req, responseFn) {
        var index = req.term.lastIndexOf(' ');
        var word = req.term.substring(index + 1);
        if (word) {
          var re = $.ui.autocomplete.escapeRegex(word);
          var matcher = new RegExp(re, 'i');
          var a = $.grep(filters, function(item, index) {
            return matcher.test(item);
          });
          responseFn(a);
        }
      },
      select: function(event, ui) {
        var query = searchBox.val();

        query = query.substring(0, query.lastIndexOf(' ') + 1) + ui.item.value;
        searchBox.val(query);
        event.preventDefault();
      },
      focus: function(event, ui) {
        event.preventDefault();
      },
      autoFocus: true
  });

  /** Bind the ENTER key with the search button and prevent tab from losing
  focus. */
  searchBox.keydown(function(event) {
    var keycode = event.keyCode ? event.keyCode : event.which;
    if (keycode == $.ui.keyCode.ENTER) {
      ApiClient.search();
    } else if (keycode == $.ui.keyCode.TAB) {
      event.preventDefault();
    }
  });
  $('a.atd-search-button').click(function() {
    ApiClient.search();
  });
};

/**
 * Binds the a original button to the list view.
 * @param {string} canonicalLink URI of the original case.
 */
google.devrel.samples.autodo.Bindings.bindOriginalButton = function(
    canonicalLink) {
  $('a.original-button').click(function() {
    window.open(canonicalLink);
    return false;
  });
};

/**
 * Binds the a reload button to the list view.
 */
google.devrel.samples.autodo.Bindings.bindReloadButton = function() {
  $('a.reload-button').click(function() {
    // Reset incident cache.
    Data.incidentListUpdated = 0;
    ApiClient.listView(ApiClient.currentListView);
  });
};

/**
 * Binds accept tag buttons to tag update handlers.
 */
google.devrel.samples.autodo.Bindings.bindAcceptTags = function() {
  $('.label').find('a.accept').click(function() {
    var incidentTags = {};
    incidentTags.accepted_tags = [];
    incidentTags.suggested_tags = [];
    incidentTags.id = parseInt($(this).parentsUntil('tr').find(
        '.content-list-div').attr('value'));
    $.each($(this).parent().siblings('.label'), function() {
      if ($(this).hasClass('accepted')) {
        incidentTags.accepted_tags.push($(this).attr('value'));
      } else {
        incidentTags.suggested_tags.push($(this).attr('value'));
      }
    });
    incidentTags.accepted_tags.push($(this).parent().attr('value'));
    var incidentTagList = {};
    incidentTagList[incidentTags.id] = incidentTags;
    ApiClient.updateTags(incidentTagList);
  });
};

/**
 * Binds remove tag buttons to tag update handers
 */
google.devrel.samples.autodo.Bindings.bindRemoveTags = function() {
  $('.label').find('a.remove').click(function() {
    var incidentTags = {};
    incidentTags.accepted_tags = [];
    incidentTags.suggested_tags = [];
    incidentTags.id = parseInt($(this).parentsUntil('tr').find(
        '.content-list-div').attr('value'));
    $.each($(this).parent().siblings('.label'), function() {
      if ($(this).hasClass('accepted')) {
        incidentTags.accepted_tags.push($(this).attr('value'));
      } else {
        incidentTags.suggested_tags.push($(this).attr('value'));
      }
    });
    var incidentTagList = {};
    incidentTagList[incidentTags.id] = incidentTags;
    ApiClient.updateTags(incidentTagList);
  });
};

/**
 * Binds settings button to a settings view.
 */
google.devrel.samples.autodo.Bindings.bindSettingsButton = function() {
  $('a.settings-button').click(function() {
    Render.settingsList();
  });
};

/**
 * Binds action to tag buttons that reveal drop down menus.
 */
google.devrel.samples.autodo.Bindings.bindTaggingButtons = function() {
  $('#assign-button').click(function() {
    $('#assign-options').show();
  });
  $('#label-button').click(function() {
    $('#label-options').show();
  });
};

/**
 * Hide pop-up menus on Blur events.
 */
google.devrel.samples.autodo.Bindings.blurPopUpMenus = function() {
  $('html').click(function() {
    $('#assign-options').hide();
    $('#label-options').hide();
  });
  $('#assign-button').click(function(event) {
    $('#label-options').hide();
    event.stopPropagation();
  });
  $('#assign-options').click(function(event) {
    event.stopPropagation();
  });
  $('#label-button').click(function(event) {
    $('#assign-options').hide();
    event.stopPropagation();
  });
  $('#label-options').click(function(event) {
    event.stopPropagation();
  });
};

/**
 * Binds visibility of notification bar to ajax start, stop, and error events.
 */
google.devrel.samples.autodo.Bindings.bindNotificationBar = function() {
  $('#notificationbar').ajaxError(function() {
    $(this).find('.message').text(
      'Unable to reach Au-to-do. ' +
      'Please check your internet connection, and try again.'
    );
    $(this).show();
  });
  $('#notificationbar').ajaxStart(function() {
    $(this).show();
  });
  $('#notificationbar').ajaxStop(function() {
    $(this).hide();
  });
};

/**
 * Binds visibility of last action bar to ajax send/sucess events.
 */
google.devrel.samples.autodo.Bindings.bindLastActionBar = function() {
  $('#lastactionbar').ajaxSend(function(event, xhr) {
    xhr.timeSent = event.timeStamp;
  });
  $('#lastactionbar').ajaxSuccess(function(event, xhr) {
    var delta = event.timeStamp - xhr.timeSent;
    var message = ['Last action took ', delta, 'ms.'].join('');
    $(this).find('.message').text(message);
    $(this).show();
  });
};

/**
 * Converts database timestamps into a slightly more readable form.
 * TODO(user): add 12-hour formating.
 * @param {string} dateStamp A timestamp in YYYY-MM-DDTHH:MM:SS format.
 * @return {string} Human-readable date string.
 */
google.devrel.samples.autodo.Util.formatDateStamp = function(dateStamp) {
  var arrDateTime = dateStamp.split('T');
  var arrDate = arrDateTime[0].split('-');
  var arrTime = arrDateTime[1].split(':');
  month = Util.removeLeadingZeroes(arrDate[1]);
  day = Util.removeLeadingZeroes(arrDate[2]);
  hour = Util.removeLeadingZeroes(arrTime[0]);
  minute = arrTime[1];
  formattedString = month + '/' + day +
                    ' ' + hour + ':' + minute;
  return formattedString;
};

/**
 * Initialize the regex used when a search is submitted.
 */
google.devrel.samples.autodo.Util.initializeRegex = function() {
  var regexes = [];
  for (var index in filters) {
    var filter = filters[index];
    regexes.push(filter);
  }
  Util.regex = new RegExp('(' + regexes.join('|') + ')');
};

/**
 * Removes leading zeroes from timestamp strings.
 * @param {string} timeStampElement string with a potential leading zero.
 * @return {string} leading zero free timestamp element.
 */
google.devrel.samples.autodo.Util.removeLeadingZeroes = function(
    timeStampElement) {
  timeStampElement = timeStampElement.replace(/^0{1}/, '');
  return timeStampElement;
};

$(document).ready(function() {
  ApiClient.DEFAULT_SEARCH = 'owner=' + currentUser;
  // Initializes UI element bindings, loads default view.
  Bindings.bindReloadButton();
  Bindings.bindSettingsButton();
  Bindings.bindTaggingButtons();
  Bindings.bindTagTextInput();
  Bindings.bindTagOptions();
  Bindings.bindAssignOptions();
  Bindings.bindNotificationBar();
  Bindings.bindLastActionBar();
  Bindings.bindSideBar();
  Bindings.bindSearchInputs();
  // Positions the pop-up option menus.
  Render.popUpPosition();
  // Loads the default view.
  ApiClient.listView();
  // Initialize the tokenizer Regex.
  Util.initializeRegex();
});

