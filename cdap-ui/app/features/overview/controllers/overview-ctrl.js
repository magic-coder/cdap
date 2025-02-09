/**
 * OverviewCtrl
 */

angular.module(PKG.name+'.feature.overview').controller('OverviewCtrl',
function ($scope, MyDataSource, $state, myLocalStorage, MY_CONFIG) {

  if(!$state.params.namespace) {
    // the controller for "ns" state should handle the case of
    // an empty namespace. but this nested state controller will
    // still be instantiated. avoid making useless api calls.
    return;
  }

  $scope.apps = [];
  $scope.datasets = [];
  $scope.streams = [];
  $scope.hideWelcomeMessage = false;

  var dataSrc = new MyDataSource($scope),
      partialPath = '/assets/features/overview/templates/',
      PREFKEY = 'feature.overview.welcomeIsHidden';

  myLocalStorage.get(PREFKEY)
    .then(function (v) {
      $scope.welcomeIsHidden = v;
    });

  $scope.hideWelcome = function () {
    myLocalStorage.set(PREFKEY, true);
    $scope.welcomeIsHidden = true;
  };

  $scope.isEnterprise = MY_CONFIG.isEnterprise;

  dataSrc.request({
    _cdapNsPath: '/apps'
  })
    .then(function(res) {
      $scope.apps = res;
    });

  dataSrc.request({
    _cdapNsPath: '/data/datasets'
  })
    .then(function(res) {
      $scope.datasets = res;
    });

  dataSrc.request({
    _cdapNsPath: '/streams'
  }, function(res) {
    if (angular.isArray(res) && res.length) {
      $scope.streams = res;
    }
  });

});
