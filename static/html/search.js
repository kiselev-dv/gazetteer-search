var gztrApp = angular.module('gztr', []);

var path = window.location.pathname;
var pathName = path.substring(0, path.lastIndexOf('/'));
var baseUrl = window.location.origin + pathName;


gztrApp.value('api_url', pathName);
gztrApp.value('search_endpoint', '/location/_search.json');
gztrApp.value('min_length', 3);
gztrApp.value('throttle_delay', 250);

gztrApp.factory('gztrSearch', ['$http', 'api_url', 'search_endpoint', 
	
	/**
	 * Very simple service, which cape the service url inside,
	 * and gives you the promise for parameters
	 * */
	
	function($http, apiurl, search){
	
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

gztrApp.controller('SearchController', 
		['$scope', 'gztrSearch', function($scope, searchAPI ) {

	$scope.link4OSMId = function(osmId) {
		var t = osmId.substring(0, 1);
		var type = (t == 'n' ? 'node': (t == 'w' ? 'way' : 'relation'));
		return 'http://osm.org/' + type + '/' + osmId.substring(1);
	}
			
			
	/**
	 * Query the searach API, throttling the queries 
	 */
	var self = this;
	
	// query string, binded 
	self.query = '';
	self.prefix = true;
	
	$scope.$watch("search.query", function(newValue){
		self.request(newValue);
	});
	
	self.request = function(q) {
		if (q && typeof q === 'string') {
			var query = q.trim();
			
			if (query.length > 2) {
				searchAPI.search({
					q: query,
					prefix: self.prefix
				}).then(self.queryResponse);
			}
		}
	};
	
	self.queryResponse = function(response) {
		self.result = response.data;
		self.result.debug_query_json = JSON.parse(self.result.debug_query);
	};
	
}]);