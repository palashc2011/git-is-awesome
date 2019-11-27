'use strict';

const helper = require('../../../lib/helpfulls.js');
const _ = require('underscore');
const rp = require('request-promise');
const app = require('../../../server/server.js');
const bookingHelper = require('./booking-helper.js');
const rateLimitUtils = require('../../../server/rate-limit/rate-limit-utils');
const { get }  = require('lodash');
const getBarCodeDataFromConsignments = async (consignmentList, organisationId, extendedModels) => {
	const orgConfig = app.mOrganisationConfig[organisationId];
	const sorterEnabled = orgConfig.sorter_enabled;
	const referenceNumWise = {};
	if (!sorterEnabled) {
		return referenceNumWise;
	} 
	const referenceNumList = consignmentList.filter(consignment => (consignment && consignment.success)).map(consignment => consignment.reference_number);
	if (!referenceNumList.length) {
		return referenceNumWise;
	}
	const consignmentsInfo = await extendedModels.Consignment.find({
		where: {
			reference_number: {
				inq: referenceNumList
			},
			organisation_id: organisationId
		},
		fields: ['reference_number', 'num_pieces', 'given_destination_location_id', 'given_origin_location_id']
	});

	const sorterInfo = orgConfig.sorter_info;	
	const defaultOriginHubCode = get(sorterInfo, 'defaultOriginPincode', '');
	const locationIds = [];
	consignmentsInfo.forEach(singleConsignment => {
		if (singleConsignment.given_destination_location_id) {
			locationIds.push(singleConsignment.given_destination_location_id);
		}
		if (singleConsignment.given_origin_location_id) {
			locationIds.push(singleConsignment.given_origin_location_id);
		}
	});
	let pincodeDetails = [];
	if (locationIds.length) {
		pincodeDetails = await extendedModels.Location.find({
			where: {
				id: {
					inq: locationIds
				},
				organisation_id: organisationId
			},
			fields: ['id', 'pincode']
		});
	}

	const idWisePincode = {};
	pincodeDetails.forEach(pincodeDetail => {
		idWisePincode[pincodeDetail.id] = pincodeDetail.pincode;
	});
	consignmentsInfo.forEach(row => {
		const totalPieces = row.num_pieces || 1;
		const barcodeArray = [];
		for (let i = 1; i <= totalPieces; i++) {
			const originPincode = idWisePincode[row.given_origin_location_id];
			const destinationPincode = idWisePincode[row.given_destination_location_id];
			const barcode = `${row.reference_number}-${i}-${row.num_pieces || 1}-${originPincode || defaultOriginHubCode}-${destinationPincode}`;
			barcodeArray.push(barcode);
		}
		referenceNumWise[row.reference_number] = barcodeArray;			
	});
	return referenceNumWise;
};
module.exports = function(CustomerIntegration) {
	CustomerIntegration.uploadSoftdata = async function (params, req, res) {
		const customerDetails = req.customerDetails || {};
		try {
			let paramsToSend = {};
			const blankIsFalse = true;
			_.extendOwn(paramsToSend, params);
			paramsToSend.consignments = paramsToSend.consignments || [];

			if (!Array.isArray(paramsToSend.consignments)) {
				throw helper.wrongInputError(`consignments should be an Array. Type ${typeof paramsToSend.consignments} is invalid.`);
			}

			paramsToSend.consignments.forEach(function (consignment) {
				if (helper.hasKey(consignment, 'eway_bill', {
						blankIsFalse: blankIsFalse
					})) {
					consignment.eway_bill = {
						ewbNumber: consignment.eway_bill,
						type: 'ewaybill_number',
					};
				}
			});

			const response = await bookingHelper.pushSoftdata(paramsToSend, req);
			const referenceNumWiseBarCodes = await getBarCodeDataFromConsignments(response.data, req.organisationId, req.extendedModels);
			response.data.forEach(function (consignment, j) {
				// Send to datadog
				if (consignment.success) {
					consignment.barCodeData = referenceNumWiseBarCodes[consignment.reference_number] || '';
					app.dogstatsd.increment('OPS_CUSTOMER_SOFTDATA_API', 1, {
						status: 'success',
						organisationId: req.organisationId,
						customerId: req.customerId,
						customerName: customerDetails.name,
						customerCode: customerDetails.code,
					});
				} else {
					app.dogstatsd.increment('OPS_CUSTOMER_SOFTDATA_API', 1, {
						status: 'failed',
						organisationId: req.organisationId,
						customerId: req.customerId,
						customerName: customerDetails.name,
						customerCode: customerDetails.code,
						errorReason: consignment.reason,
						errorMessage: consignment.message,
					});
				}

				// Handle pieces
				if (paramsToSend.consignments[j].pieces_detail && consignment.success) {
					let numOfPieces = paramsToSend.consignments[j].pieces_detail.length || 0;
					if (numOfPieces !== 0) {
						response.data[j].pieces = [];
						for (let i = 0; i < numOfPieces; i++) {
							let pieceElem = {};
							pieceElem.reference_number = consignment.reference_number + ('000' + (i + 1)).substr(-3, 3);
							consignment.pieces.push(pieceElem);
						}
					}
				}
			});

			return response;
		}
        catch(err) {
			// Send to datadog
			app.dogstatsd.increment('OPS_CUSTOMER_SOFTDATA_API_BULK_ERROR', 1, {
				organisationId: req.organisationId,
				customerId: req.customerId,
				customerName: customerDetails.name,
				customerCode: customerDetails.code,
				errorCode: err.errorCode || 500,
				errorReason: err.reason,
			});
			throw err;
		}
    };
	
	CustomerIntegration.generateMultiPieceLabel = Promise.coroutine(function* (params, req, res) {
		let extendedModels = req.extendedModels;
		if (!params.reference_number) {
			throw helper.wrongInputError('Reference number is mandatory');
		}

		let consignment = yield extendedModels.Consignment.findOne({
			where: {
				reference_number: params.reference_number,
				organisation_id: req.organisationId,
			}
		});

		if (!consignment) {
			throw helper.wrongInputError('Invalid Reference Number');
		}

		let reference_numbers = [];
		reference_numbers.push(params.reference_number);
		let requestParams = {
			url: app.mApplicationConfig.BUSINESS_BOOKING_BACKEND_URL + '/print/multipiece/getLabel',
			method: 'POST',
			json: {
				referenceNumbers: reference_numbers,
				isSmall: true,
			},
			headers: {
				'api-key': '8tskcdaRpARZQTu2JCjMAmdBBkpEsk2u'
			},
			resolveWithFullResponse: true,
			simple: false
		};
		let response = yield rp.post(requestParams);

		if (response.statusCode !== 201) {
			helper.logErrorToSentry(response.body.error, null, null, null, requestParams);
			throw helper.errorCreator({
				statusCode: response.statusCode,
				message: response.body.error.message
			});
		}

		return response.body;
	});

	CustomerIntegration.trackShipmentForCustomer = async function(referenceNumber, req, res){
		const organisationId = req.organisationId;
		const customerId = req.customerId;
		const customerDetails = req.customerDetails || {};
		try {
			if (!organisationId) {
				throw helper.wrongInputError('OrganisationId should be present');
			}

			if (!customerId) {
				throw helper.wrongInputError('Customer Not found');
			}

			if (!referenceNumber) {
				throw helper.wrongInputError('', {
					message: 'Reference number should be present',
					reason: 'REFERENCE_NUMBER_EMPTY',
				});
			}
			referenceNumber = helper.sanitizeStringCode(referenceNumber);

			const extendedModels = req.extendedModels;
			let toRet = {};
			const consignmentQueryToExecute = 'SELECT consignment.reference_number AS reference_number,' +
				' consignment.id AS consignment_id,' +
				' consignment.service_type_id AS service_type_id,' +
				' consignment.attempt_count AS attempt_count,' +
				' consignment.status AS status,' +
				' consignment.delivery_kyc_type AS delivery_kyc_type,' +
				' consignment.delivery_kyc_number AS delivery_kyc_number,' +
				' COALESCE(consignment.is_cod, FALSE) AS is_cod,' +
				' COALESCE(consignment.cod_amount, 0) AS cod_amount,' +
				' consignment.weight AS weight,' +
				' EXTRACT(EPOCH FROM consignment.created_at) * 1000 AS creation_date,' +
				' consignment.given_origin_location_id AS given_origin_location_id,' +
				' consignment.given_destination_location_id AS given_destination_location_id,' +
				' hub.code AS hub_code' +
				' FROM consignment' +
				' INNER JOIN customer on consignment.customer_id = customer.id' +
				' LEFT JOIN hub ON hub.id = consignment.hub_id' +
				' WHERE consignment.reference_number = $1' +
				' AND consignment.organisation_id = $2' +
				' AND (customer.id = $3 OR customer.parent_id = $3)';

			const requiredConsignments = await helper.executeQueryAsync(extendedModels.Consignment, consignmentQueryToExecute, [referenceNumber, organisationId, customerId]);

			if (requiredConsignments.length === 0) {
				throw helper.wrongInputError('Reference number is not valid');
			}

			toRet = requiredConsignments[0];

			const consignmentEventQuery = 'SELECT consignmentevent.type AS type,' +
				' extract(epoch from consignmentevent.event_time) * 1000 AS event_time,' +
				' hub.name AS hub_name,' +
				' hub.code AS hub_code,' +
				' task.poc_image AS poc_image,' +
				' task.signature_image AS signature_image,' +
				' task.failure_reason AS failure_reason' +
				' FROM consignmentevent' +
				' LEFT JOIN hub ON consignmentevent.hub_id = hub.id' +
				' LEFT JOIN task ON task.id = consignmentevent.task_id' +
				' WHERE consignmentevent.consignment_id = $1' +
				' AND consignmentevent.is_rejected IS NOT TRUE' +
				' AND consignmentevent.event_time IS NOT NULL' +
				' ORDER BY event_time DESC';

			const consignmentEvents = await helper.executeQueryAsync(extendedModels.ConsignmentEvent, consignmentEventQuery, [toRet.consignment_id]);

			const eventsToSend = [];
			consignmentEvents.forEach(function (elem) {
				const eventElem = {
					type: elem.type,
					event_time: elem.event_time,
					// worker_name: elem.worker_name,
					// worker_code: elem.worker_code,
					hub_name: elem.hub_name,
					hub_code: elem.hub_code,
					// lat: elem.lat,
					// lng: elem.lng,
					failure_reason: null
				};
				if (elem.type === 'delivered' || elem.type === 'attempted') {
					eventElem.poc_image = elem.poc_image;
				} else {
					eventElem.poc_image = null;
				}

				if (elem.type === 'attempted') {
					eventElem.failure_reason = elem.failure_reason;
				}

				if (elem.type === 'delivered') {
					eventElem.signature_image = elem.signature_image;
				}

				eventsToSend.push(eventElem);
			});
			toRet.events = eventsToSend;
			delete toRet.given_origin_location_id;
			delete toRet.given_destination_location_id;
			delete toRet.consignment_id;

			// Datadog
			app.dogstatsd.increment('OPS_CUSTOMER_TRACKING_API', 1, {
				status: 'success',
				organisationId: organisationId,
				customerId: customerId,
				customerName: customerDetails.name,
				customerCode: customerDetails.code,
			});

			return toRet;
		} catch(err) {
			app.dogstatsd.increment('OPS_CUSTOMER_TRACKING_API', 1, {
				status: 'failed',
				organisationId: organisationId,
				customerId: customerId,
				customerName: customerDetails.name,
				customerCode: customerDetails.code,
				errorCode: err.errorCode || 500,
				errorReason: err.reason,
			});
			throw err;
		}
	};

	CustomerIntegration.printShipmentLabelStream = async function(referenceNumber, isSmall, req, res) {
		const extendedModels = req.extendedModels;
		const customerId = req.customerId;
		const organisationId = req.organisationId;
		const customerDetails = req.customerDetails;
		try {
			referenceNumber = helper.sanitizeStringCode(referenceNumber);
			const requiredConsignments = await  extendedModels.Consignment.findOne({
				where: {
					reference_number: referenceNumber,
					organisation_id: req.organisationId,
					customer_id: customerId
				}
			});
			if (!requiredConsignments) {
				throw helper.wrongInputError('', {
					message: 'Consignment does not belong to customer',
					reason: 'CONSIGNMENT_NOT_FOUND',
				});
			}
			res.set('Content-disposition', `attachment;filename=CN${referenceNumber}.pdf`);
			let isSmallParameter = isSmall === 'true' ? true : false; false;
			await extendedModels.Consignment.getLabelsForConsignmentStream(req.organisationId, extendedModels, [referenceNumber], false, res, isSmallParameter);
			
			// Datadog
			app.dogstatsd.increment('OPS_CUSTOMER_STREAM_LABEL_API', 1, {
				status: 'success',
				outputType: 'stream',
				organisationId: organisationId,
				customerId: customerId,
				customerName: customerDetails.name,
				customerCode: customerDetails.code,
			});
		} catch(err) {
			app.dogstatsd.increment('OPS_CUSTOMER_STREAM_LABEL_API', 1, {
				status: 'failed',
				outputType: 'stream',
				organisationId: organisationId,
				customerId: customerId,
				customerName: customerDetails.name,
				customerCode: customerDetails.code,
				errorCode: err.errorCode || 500,
				errorReason: err.reason,
			});
			throw err;
		}
	};

	CustomerIntegration.printShipmentLabelUrl = async function(referenceNumber, req, res, isSmall) {
		const extendedModels = req.extendedModels;
		const organisationId = req.organisationId;
		const customerId = req.customerId;
		const customerDetails = req.customerDetails;
		try {
			referenceNumber = helper.sanitizeStringCode(referenceNumber);
			const requiredConsignments = await extendedModels.Consignment.findOne({
				where: {
					reference_number: referenceNumber,
					organisation_id: organisationId,
					customer_id: customerId
				}
			});
			if (!requiredConsignments) {
				throw helper.wrongInputError('', {
					message: 'Consignment does not belong to customer',
					reason: 'CONSIGNMENT_NOT_FOUND',
				});
			}
			let isSmallParameter = isSmall === 'true' ? true : false;
			const shippingLabelUrl = await helper.getPublicUrlForShippingLabel(referenceNumber, organisationId, isSmallParameter);
			
			// Datadog
			app.dogstatsd.increment('OPS_CUSTOMER_LABEL_URL_API', 1, {
				status: 'success',
				organisationId: organisationId,
				customerId: customerId,
				customerName: customerDetails.name,
				customerCode: customerDetails.code,
			});
			
			return {
				status: 'OK',
				data: {
					url: shippingLabelUrl
				}
			};
		} catch(err) {
			app.dogstatsd.increment('OPS_CUSTOMER_LABEL_URL_API', 1, {
				status: 'failed',
				organisationId: organisationId,
				customerId: customerId,
				customerName: customerDetails.name,
				customerCode: customerDetails.code,
				errorCode: err.errorCode || 500,
				errorReason: err.reason,
			});
			throw err;
		}
		
	};

	CustomerIntegration.remoteMethod(
		'printShipmentLabelStream', {
			description: 'print shipping label external',
			accepts: [
				{ arg: 'reference_number', type: 'string', http: { source: 'query' } },
				{arg: 'is_small', type: 'string', http: { source: 'query' }},
				{arg: 'req', type: 'object', http: {source: 'req'}},
				{arg: 'res', type: 'object', http: {source: 'res'}}
			],
			returns: {
				description: 'Response gives back the status. With OK as its value.',
				arg: 'status',
				root: true
			},
			http: {
				path: '/consignment/shippinglabel/stream',
				verb: 'get',
				status: 200
			},
		}
	);

	CustomerIntegration.remoteMethod(
		'printShipmentLabelUrl', {
			description: 'print shipping label external',
			accepts: [
				{ arg: 'reference_number', type: 'string', http: { source: 'query' } },
				{arg: 'req', type: 'object', http: {source: 'req'}},
				{arg: 'res', type: 'object', http: {source: 'res'}},
				{ arg: 'is_small', type: 'string', http: { source: 'query' } }
			],
			returns: {
				description: 'Response gives back the status. With OK as its value.',
				arg: 'status',
				root: true
			},
			http: {
				path: '/consignment/shippinglabel/link',
				verb: 'get',
				status: 200
			},
		}
	);

	CustomerIntegration.beforeRemote('trackShipmentForCustomer', rateLimitUtils.beforeRemoteForRateLimit({
		rateLimiter: 'customer_tracking',
		keyFn: (ctx) => ctx.req.customerId,
	}));
	CustomerIntegration.remoteMethod(
		'trackShipmentForCustomer', {
			description: 'Api to track shipment for organisation',
			accepts: [
				{ arg: 'reference_number', type: 'string', http: { source: 'query' } },
				{arg: 'req', type: 'object', http: {source: 'req'}},
				{arg: 'res', type: 'object', http: {source: 'res'}}
			],
			returns: {
				description: 'Response gives back the status. With OK as its value.',
				root: true
			},
			http: {
				path: '/consignment/track',
				verb: 'get',
				status: 200
			}
		}
	);

    CustomerIntegration.remoteMethod(
		'uploadSoftdata', {
			description: 'Upload softdata of consignments',
			accepts: [
				{
					arg: 'params',
					type: 'object',
					required: true,
					http: {
						source: 'body'
					}
				},
				{arg: 'req', type: 'object', http: {source: 'req'}},
				{arg: 'res', type: 'object', http: {source: 'res'}}
			],
			returns: {
				description: 'Response gives back the status. With OK as its value.',
				arg: 'status',
				root: true
			},
			http: {
				path: '/consignment/softdata',
				verb: 'post',
				status: 200
			},
		}
	);

	CustomerIntegration.remoteMethod(
		'generateMultiPieceLabel', {
			description: 'Generate multi-piece Consignment Labels',
			accepts: [
				{ arg: 'params', type: 'object', required: true, http: { source: 'body' } },
				{ arg: 'req', type: 'object', http: { source: 'req' } },
				{ arg: 'res', type: 'object', http: { source: 'res' } }
			],
			returns: {
				arg: 'data',
				root: true,
				description: 'Response'
			},
			http: {
				path: '/consignment/label/multipiece',
				verb: 'post',
				status: 200,
			}
		}
	);
};
