

define([
	'lib/text!../../templates/modal.html'
	], function (Template) {

		return Em.View.create({
			template: Em.Handlebars.compile(Template),
			classNames: ['modal', 'hide', 'fade'],
			elementId: 'modal-from-dom',
			show: function (title, body, callback) {
				this.set('title', title);
				this.set('body', body);

				this.set('confirmed', function () {
					App.Views.Modal.hide();
					callback();
				});

				var el = $(this.get('element'));
				el.modal('show');

			},
			hide: function () {
				var el = $(this.get('element'));
				el.modal('hide');
			}
		}).append();
	});