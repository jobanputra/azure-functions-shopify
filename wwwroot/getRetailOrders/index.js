var request = require('request');
var _ = require('lodash');
var azureStorage = require('azure-storage');

module.exports = function (context, myTimer) {
    var currentTime = new Date();

    var accountName = process.env["BlobStorageAccountname"];
    var key = process.env["BlobStorageAccountKey"];
    var containerName = process.env["BlobStorageContainername"];
    var statusFilename = process.env["StatusFilename-Retail"];
    var ordersFilename = process.env["OrdersFilename-Retail"];
    var blobService = blobService = azureStorage.createBlobService(accountName, key);

    var fields = 'id,email,closed_at,created_at,updated_at,number,token,total_price,financial_status,cancel_reason,user_id,processed_at,phone,order_number,processing_method,source_name,billing_address,customer';
    var shopifyAlias = process.env["ShopifyAlias-Retail"];
    var shopifyUsername = process.env["ShopifyUsername-Retail"];
    var shopifyPassword = process.env["ShopifyPassword-Retail"];
    var shopifyApiUrl = 'https://' + shopifyUsername + ':' + shopifyPassword +
        '@' + shopifyAlias + '.myshopify.com/admin/orders.json?limit=250&status=any' +
        '&fields=' + fields;

    initBlobContainer(onStorageInitError, function () {
        initStatusBlob(onStorageInitError, function () {
            initOrdersBlob(onStorageInitError, function () {
                context.log('Blob storage successfully initialized');
                //get the last time this function ran successfully
                getLastExecutionTime(onStorageInitError, function (lastUpdateTime) {
                    //Successfully retreieved last execution time
                    context.log('Retrieved last execution time: ', lastUpdateTime);
                    shopifyApiUrl = shopifyApiUrl + '&created_at_min=' + lastUpdateTime;
                    getOrderData(shopifyApiUrl, 1, 0, onShopifyOrderLoadingError);
                });
            });
        });
    });

    function writeOrderdata(orders, pageNumber, totalOrderCount) {
        context.log.verbose('Received data for page: ', pageNumber)
        appendOrdersData(orders, onStorageInitError, function () {
            context.log.verbose('Append complete for page: ', pageNumber);
            pageNumber++;
            getOrderData(shopifyApiUrl, pageNumber, totalOrderCount, onShopifyOrderLoadingError);
            // context.done();

        });
    }

    function onAllOrdersReceived(totalOrderCount) {
        context.log('Retieved ' + totalOrderCount + ' orders');
        updateLastExecutionTime(currentTime.toISOString(), onStorageInitError, function () {
            context.log('Successfully set last execute time');
            context.done();
        });
    }

    function onStorageInitError(error) {
        context.log.error('Error when initializing blob storage');
        context.log.error(error);
        context.done();
    }

    function onShopifyOrderLoadingError(error) {
        context.log('onError invoked');
        context.log(error);
        context.done();
    }

    function getOrderData(url, pageNumber, totalOrderCount, onError) {
        context.log.verbose('Executing for page: ' + pageNumber);
        pagedUrl = url + '&page=' + pageNumber;
        request(pagedUrl, function (error, response, body) {
            if (error) {
                onError(error);
            } else {
                var orders = JSON.parse(body).orders;
                totalOrderCount += orders.length;
                if (orders.length == 0) {
                    onAllOrdersReceived(totalOrderCount);
                }
                else {
                    writeOrderdata(orders, pageNumber, totalOrderCount);
                }
            }
        });
    }

    function appendOrdersData(orders, onError, onSuccess) {
        let csv = '';
        _.each(orders, function (order) {
            //billing_address is optional field. Create a null object if it does not exist for the CSV to work
            if (!order.hasOwnProperty('billing_address')) {
                order.billing_address = null;
            }
            if(!order.hasOwnProperty('customer')){
                order.customer=null;
            }
            var keys = _.sortBy(Object.keys(order))
            _.forEach(keys, function (key) {
                value = order[key];
                let valueToWrite = value;
                switch (key) {
                    case 'billing_address':
                        valueToWrite = ''
                        if (value != null) {
                            valueToWrite = value.province_code;
                        }
                        break;
                    case 'customer':
                        valueToWrite = ',,,'
                        if (value != null) {
                            //Some fields have commas in them
                            let firstName = (value.first_name == null) ? '':value.first_name.replace(',','');
                            let lastName = (value.last_name == null )? '': value.last_name.replace(',','')
                            valueToWrite =  firstName + ',' + lastName + ',' + value.phone + ',' + value.orders_count
                        }
                    default:
                        break;
                }
                csv = csv + valueToWrite + ',';
            });
            csv = csv.slice(0, -1) + '\r\n';
        });

        blobService.appendFromText(containerName, ordersFilename, csv,
            function (error, result, response) {
                if (!error) {
                    onSuccess();
                } else {
                    context.log.error('Unable to append to order details');
                    onError(error)
                };
            });
    }

    function updateLastExecutionTime(dateString, onError, onSuccess) {
        blobService.createBlockBlobFromText(containerName, statusFilename, dateString,
            function (error, result) {
                if (!error) {
                    onSuccess();
                } else {
                    context.log.error('Unable to update last execution time');
                    onError(error);
                }
            });
    }

    function getLastExecutionTime(onError, onSuccess) {
        blobService.getBlobToText(containerName, statusFilename, function (error, text) {
            if (!error) {
                onSuccess(text);
            }
            else {
                context.log.error('Unable to retrieve last execution time');
                context.log.error(error);
            }
        });
    }

    function initBlobContainer(onError, onSuccess) {
        //create the container to hold the data
        blobService.createContainerIfNotExists(containerName, function (error, result, response) {
            if (!error) {
                context.log.verbose("Container exists/created");
                onSuccess();
            } else {
                //Error when creating container
                context.log.error('Error when creating container');
                onError(error);
            }
        });
    }

    function initOrdersBlob(onError, onSuccess) {
        blobService.doesBlobExist(containerName, ordersFilename,
            function (error, result, response) {
                if (!error) {
                    if (!result.exists) {
                        context.log('Orders file does not exist');
                        let header = '';
                        _.forEach(_.sortBy(_.split(fields, ',')), function(field){
                            header = header + field + ','
                        });
                        header = header.slice(0, -1)
                            .replace('billing_address', 'billingState')
                            .replace('customer', 'firstname,lastname,customerphone,orders_count')
                            + '\r\n';
                        blobService.createAppendBlobFromText(containerName, ordersFilename, header,
                            function (error, result, response) {
                                if (!error) {
                                    context.log('Successfully created order file');
                                    onSuccess();
                                } else {
                                    //Error when creating order status file
                                    context.log.error('Error creating order file');
                                    onError(error);
                                }
                            });
                    } else {
                        context.log('Orders file exists.');
                        onSuccess();
                    }
                } else {
                    context.log.error('Error when checking for orders file');
                    onError(error);
                }
            });
    }

    function initStatusBlob(onError, onSuccess) {
        blobService.doesBlobExist(containerName, statusFilename,
            function (error, result, response) {
                context.log.verbose("initStatusBlob doesBlobExists completed.");
                if (!error) {
                    if (!result.exists) {
                        context.log('Order status file does not exist');
                        let dateString = new Date(1980, 00, 01).toISOString();
                        updateLastExecutionTime(dateString, function (error) {
                            context.log.error('Error creating status file');
                            onError(error);
                        }, function () {
                            context.log('Successfully created order status file');
                            onSuccess();
                        });
                    } else {
                        context.log('Order status file exists.');
                        onSuccess();
                    }
                } else {
                    context.log.error('Error when checking for order status file')
                    onError(error);
                }
            });
    }

}
