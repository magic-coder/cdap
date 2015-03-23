angular.module(PKG.name + '.feature.etlapps')
  .controller('ETLAppsCreateController', function($scope, $filter) {
    var filterFilter = $filter('filter');
    $scope.displayName = 'ETL App 1';
    $scope.description = '';
    var i = 0;
    $scope.properties = [
      {
        key: 'key1',
        value: 'value1'
      },
      {
        key: 'key2',
        value: 'value2'
      }
    ];

    $scope.addProperties = function() {
      $scope.properties.push({
        key: 'Property ' + i,
        value: ''
      });
      i+=1;
    };

    $scope.removeProperty = function(property) {
      var match = filterFilter($scope.properties, property);
      if (match.length) {
        $scope.properties.splice($scope.properties.indexOf(match[0]), 1);
      }
    };

    $scope.types = [
      {
        name: 'Stream'
      },
      {
        name: 'Dataset'
      }
    ];

    $scope.filetypes = [
      {
        name: 'CSV'
      },
      {
        name: 'AVRO'
      }
    ];

    $scope.schema = {
      fields: [
        {
          name: 'Field1',
          type: 'text'
        }
      ]
    };

    $scope.outputSchema = {
      fields: [
        {
          name: 'OutputField1',
          type: 'text'
        }
      ]
    };

    $scope.addSchemaFields = function() {
      $scope.schema.fields.push({
        name: 'Field' + Date.now(),
        type: ''
      });
    };

    $scope.removeSchemaField = function(field) {
      var matchIndex,
          value;

      for (var i =0; i<$scope.schema.fields.length; i++) {
        value = $scope.schema.fields[i];
        if (value.name === field.name) {
          matchIndex = i;
          break;
        }
      }
      if (matchIndex) {
        $scope.schema.fields.splice(matchIndex, 1);
      }
    }

    $scope.addOutputSchemaFields = function() {
      $scope.outputSchema.fields.push({
        name: 'OuputField' + Date.now(),
        type: ''
      });
    };

    $scope.removeOutputSchemaField = function(field) {
      var matchIndex,
          value;

      for (var i =0; i<$scope.outputSchema.fields.length; i++) {
        value = $scope.outputSchema.fields[i];
        if (value.name === field.name) {
          matchIndex = i;
          break;
        }
      }
      if (matchIndex) {
        $scope.outputSchema.fields.splice(matchIndex, 1);
      }
    }

  });
