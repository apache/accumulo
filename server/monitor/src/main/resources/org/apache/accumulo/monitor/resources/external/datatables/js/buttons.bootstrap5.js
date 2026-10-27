/*! Buttons Bootstrap 5 styling 4.0.1 for DataTables
 * Copyright (c) SpryMedia Ltd - datatables.net/license
 */

(function(factory){
	if (typeof define === 'function' && define.amd) {
		// AMD
		define(['datatables.net-bs5', 'datatables.net-buttons'], function (dt) {
			return factory(window, document, dt);
		});
	}
	else if (typeof exports === 'object') {
		// CommonJS
		var cjsRequires = function (root) {
			if (! root.DataTable) {
				require('datatables.net-bs5')(root);
			}

			if (! window.DataTable.Buttons) {
				require('datatables.net-buttons')(root);
			}
		};

		if (typeof window === 'undefined') {
			module.exports = function (root) {
				if (! root) {
					// CommonJS environments without a window global must pass a
					// root. This will give an error otherwise
					root = window;
				}

				cjsRequires(root);
				return factory(root, root.document, root.DataTable);
			};
		}
		else {
			cjsRequires(window);
			module.exports = factory(window, window.document, window.DataTable);
		}
	}
	else {
		// Browser
		factory(window, document, window.DataTable);
	}
}(function(window, document, DataTable) {
	'use strict';



	var Dom = DataTable.Dom;
	var util = DataTable.util;

	util.object.assignDeep(DataTable.Buttons.defaults, {
		dom: {
			container: {
				className: 'dt-buttons btn-group flex-wrap'
			},
			button: {
				className: 'btn btn-secondary',
				active: 'active',
				dropHtml: '',
				dropClass: 'dropdown-toggle'
			},
			collection: {
				container: {
					tag: 'div',
					className: 'dt-button-collection',
					content: {
						tag: 'ul',
						className: 'dropdown-menu show'
					}
				},
				closeButton: false,
				button: {
					tag: 'li',
					className: 'dt-button',
					active: 'dt-button-active-a',
					disabled: 'disabled',
					liner: {
						tag: 'a',
						className: 'dropdown-item'
					},
					spacer: {
						className: 'divider',
						tag: 'li'
					}
				}
			},
			split: {
				action: {
					tag: 'a',
					className: 'btn btn-secondary dt-button-split-drop-button',
					closeButton: false
				},
				dropdown: {
					tag: 'button',
					className:
						'btn btn-secondary dt-button-split-drop dropdown-toggle-split',
					closeButton: false,
					align: 'split-left',
					splitAlignClass: 'dt-button-split-left'
				},
				wrapper: {
					tag: 'div',
					className: 'dt-button-split btn-group',
					closeButton: false
				}
			}
		},
		buttonCreated: function (config, button, inCollection) {
			return config.buttons && ! inCollection
				? Dom.c('div').classAdd('btn-group a').append(button)
				: button;
		}
	});

	DataTable.ext.buttons.collection.rightAlignClassName = 'dropdown-menu-right';


	return DataTable;
}));
