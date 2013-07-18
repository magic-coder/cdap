/*
 * List Controller
 */

define([], function () {

	var Controller = Em.Controller.extend({

		elements: Em.Object.create(),

		__titles: {
			'App': 'Applications',
			'Flow': 'Process',
			'Batch': 'Process',
			'Stream': 'Collect',
			'Procedure': 'Query',
			'Dataset': 'Store'
		},
		__plurals: {
			'App': 'apps',
			'Flow': 'flows',
			'Batch': 'mapreduce',
			'Stream': 'streams',
			'Procedure': 'procedures',
			'Dataset': 'datasets'
		},
		entityTypes: new Em.Set(),
		title: function () {
			return this.__titles[this.get('entityType')];
		}.property('entityType'),
		load: function (type) {

			var self = this;
			this.set('entityType', type);

			this.entityTypes.add(type);

			this.HTTP.get('rest', this.__plurals[type], function (objects) {

				var i = objects.length;
				while (i--) {
					objects[i] = C[type].create(objects[i]);
				}

				self.set('elements.' + type, Em.ArrayProxy.create({content: objects}));

				self.interval = setInterval(function () {
					self.updateStats();
				}, C.POLLING_INTERVAL);

				/*
				 * Give the chart Embeddables 100ms to configure
				 * themselves before updating.
				 */
				setTimeout(function () {
					self.updateStats();
				}, C.EMBEDDABLE_DELAY);

			});

		},

		updateStats: function () {

			var content, self = this, models = [];
			for (var j=0; j<this.entityTypes.length; j++) {
				var objects = this.get('elements.' + this.entityTypes[j]);
				if (objects) {
					models = models.concat(objects.content);
				}
			}

			/*
			 * Hax until we have a pub/sub system for state.
			 */
			i = models.length;
			while (i--) {
				if (typeof models.updateState === 'function') {
					models.updateState(this.HTTP);
				}
			}
			/*
			 * End hax
			 */

			// Scans models for timeseries metrics and updates them.
			C.Util.updateTimeSeries(models, this.HTTP);

			// Scans models for aggregate metrics and udpates them.
			C.Util.updateAggregates(models, this.HTTP);

		},

		unload: function () {

			clearInterval(this.interval);

			this.set('elements', Em.Object.create());

		}

	});

	Controller.reopenClass({
		type: 'List',
		kind: 'Controller'
	});

	return Controller;

});