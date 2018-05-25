var gztrApp = angular.module('gztr', []);

var path = window.location.pathname;
var pathName = path.substring(0, path.lastIndexOf('/'));
var baseUrl = window.location.origin + pathName;

gztrApp.value('api_url', pathName);
gztrApp.value('search_endpoint', '/location/_search.json');
gztrApp.value('min_length', 3);
gztrApp.value('throttle_delay', 250);
gztrApp.value('throttle_culdown', 500);

gztrApp.factory('gztrSearch', ['$http', 'api_url', 'search_endpoint', 
	function($http, apiurl, search){
	/**
	 * Very simple service, which cape the service url inside,
	 * and gives you the promise for parameters
	 * */
	
	var searchApi = {};
	var url = apiurl + search;
	
	searchApi.search = function(params) {
		
		return $http({
		    url: url, 
		    method: "GET",
		    params: params
		});
		
	};
	
	return searchApi;
	
}]);

gztrApp.factory('gztrThrottle', ['$timeout', 'throttle_delay', 'throttle_culdown',
	function($timeout, throttle_delay, throttle_culdown) {
	
	var delay = null;
	var subscribers = [];
	var culdown = null;
	var waiting = null;
	
	function submit(data) {
		
		if (delay) {
			$timeout.cancel(delay);
			delay = null;
		}
		
		delay = $timeout(_sendNow.bind(this, data), throttle_delay);
	}

	function _sendNow(data) {
		// If culdown isn't expire yet, update waiting data
		if (culdown) {
			waiting = data;
		}
		// We are free to send our data right away
		else {
			_notify(data);
			_restartCuldown(data);
		}
	}
	
	function _restartCuldown(data) {
		// Start culdown after last data been send
		
		// remove previous timer
		if(culdown) {
			$timeout.cancel(culdown);
			culdown = null;
		} 
		// reset waiting data
		waiting = null;

		culdown = $timeout(function() {
			culdown = null;
			// Waiting data been updated wile we've wait in culdown
			if (waiting) {
				// Send it without delay, but with culdown update
				_sendNow(waiting);
			}
		}, throttle_culdown);
	}
	
	function _notify(data) {
		subscribers.forEach(function(s) {
			s(data);
		});
	}
	
	function subscribe(s) {
		subscribers.push(s);
	}
	
	return {
		subscribe: subscribe,
		submit: submit
	};
	
}]);

gztrApp.filter('trim', function() {
	return function (arr, trim) {
		if (arr) {
			return arr.filter(function(item, index){
				return index < trim;
			});
		}
		return arr;
	};
});

gztrApp.controller('SearchController', 
		['$scope', 'gztrThrottle', 'gztrSearch', 
			function($scope, throttle, searchAPI) {

	$scope.link4OSMId = function(osmId) {
		var t = osmId.substring(0, 1);
		var type = (t == 'n' ? 'node': (t == 'w' ? 'way' : 'relation'));
		return 'http://osm.org/' + type + '/' + osmId.substring(1);
	}
			
			
	/**
	 * Query the searach API, throttling the queries 
	 */
	var self = this;
	
	$scope.geolocate = function() {
		navigator.geolocation.getCurrentPosition(function(position) {
			$scope.$apply(function() {
				var lat = Math.round(position.coords.latitude * 10000) / 10000;
				var lon = Math.round(position.coords.longitude * 10000) / 10000;
				self.latlon =  lat + '/' + lon;
			});
		});
	}
	
	// query string, binded 
	self.query = '';
	self.prefix = true;
	self.requestCounter = 0;
	
	$scope.$watch("search.query", function(newValue) {
		self.request(newValue);
	});
	
	$scope.$watch("search.prefix", function() {
		self.request(self.query);
	});
	
	self.request = function(q) {
		self.requestCounter++;
		if (q && typeof q === 'string') {
			var query = q.trim();
			
			var split = query.split(' ');
			var last = split[split.length - 1];
			
			var params = {
				q: query,
				prefix: self.prefix,
				mark: self.requestCounter
			};
			
			if (self.latlon) {
				var ll = self.latlon.split(/[\/ ,]/);
				params['lat'] = parseFloat(ll[0]);
				params['lon'] = parseFloat(ll[1]);
			}
			
			if (query.length > 2 && last.length > 1 || /\d/.test(last)) {
				throttle.submit(params);
			}
		}
	};

	throttle.subscribe(function(data) {
		searchAPI.search(data).then(self.queryResponse);
	});
	
	self.queryResponse = function(response) {
		if (!self.result || parseInt(response.data.mark) > parseInt(self.result.mark)) {
			self.result = response.data;
			self.result.debug_query_json = JSON.parse(self.result.debug_query);
		}
	};
	
}]);