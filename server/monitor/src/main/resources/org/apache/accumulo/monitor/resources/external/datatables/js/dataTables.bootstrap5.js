/*! DataTables Bootstrap 5 integration
 * © SpryMedia Ltd - datatables.net/license
 */

(function(factory){
	if (typeof define === 'function' && define.amd) {
		// AMD
		define(['datatables.net'], function (dt) {
			return factory(window, document, dt);
		});
	}
	else if (typeof exports === 'object') {
		// CommonJS
		var cjsRequires = function (root) {
			if (! root.DataTable) {
				require('datatables.net')(root);
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


	/* Set the defaults for DataTables initialisation */
	DataTable.util.object.assignDeep(DataTable.defaults, {
		renderer: 'bootstrap'
	});
	/* Default class modification */
	DataTable.util.object.assignDeep(DataTable.ext.classes, {
		container: 'dt-container dt-bootstrap5',
		search: {
			input: 'form-control form-control-sm'
		},
		length: {
			select: 'form-select form-select-sm'
		},
		processing: {
			container: 'dt-processing card'
		},
		layout: {
			row: 'row mt-2 justify-content-between',
			cell: 'd-md-flex justify-content-between align-items-center',
			tableCell: 'col-12',
			start: 'dt-layout-start col-md-auto me-auto',
			end: 'dt-layout-end col-md-auto ms-auto',
			full: 'dt-layout-full col-md'
		}
	});
	/* Bootstrap paging button renderer */
	DataTable.ext.renderer.pagingButton.bootstrap = function (settings, buttonType, content, active, disabled) {
		var btnClasses = ['dt-paging-button', 'page-item'];
		if (active) {
			btnClasses.push('active');
		}
		if (disabled) {
			btnClasses.push('disabled');
		}
		var li = DataTable.Dom.c('li').classAdd(btnClasses.join(' '));
		var a = DataTable.Dom
			.c('button')
			.classAdd('page-link')
			.attr('role', 'link')
			.attr('type', 'button')
			.html(content)
			.appendTo(li);
		return {
			display: li.get(0),
			clicker: a.get(0)
		};
	};
	DataTable.ext.renderer.pagingContainer.bootstrap = function (settings, buttonEls) {
		return DataTable.Dom
			.c('ul')
			.classAdd('pagination')
			.append(buttonEls)
			.get(0);
	};


	return DataTable;
}));
