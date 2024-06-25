using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Sunp.Api.Client;
using SunpAgent;
using SunpAgent.Helpers;
using SunpAgent.Veeder;
using System;
using System.Text.Json;
using System.Net.Http.Headers;
using static SunpAgent.VeederClient;

public class Program {
    private static async Task Main(string[] args) {
        Console.WriteLine("Application is running. Close the window to exit...");
        Console.CancelKeyPress += (sender, eventArgs) => {
            Console.WriteLine("Ctrl + C is pressed. But application will not exit. Close the window to exit.");
            eventArgs.Cancel = true;
        };
        while (true) {
            try {
                using var loggerFactory = LoggerFactory.Create(builder => {
                    builder.AddConsole();
                });
                var builder = new ConfigurationBuilder().AddJsonFile($"appsettings.json", true, true);
                var config = builder.Build();

                var logger = loggerFactory.CreateLogger<VeederClient>();

                var vedeerSettings = config.GetSection("VedeerSettings").Get<VeederSettings>()
                     ?? throw new Exception("Unable to find config section 'VedeerSettings'");
                var veederClient = new VeederClient(logger, vedeerSettings);
                logger.LogInformation("VedeerClient initialized");

                var httpClient = new HttpClient();
                httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", config["SunpApiClient:BearerToken"]);
                var sunpApiClient = new SunpApiClient(config["SunpApiClient:Url"], httpClient);
                logger.LogInformation("SunpApiClient initialized");

                var collectedTankMeasurements = new List<TankMeasurementData>();
                var collectedTankTransfers = new List<TankTransferData>();

                logger.LogInformation("Start collecting data...");
                await veederClient.Start((result) => {
                    logger.LogInformation("Veeder data received");
                    _ = processMeasurements(
                        result.SourceValues,
                        collectedTankMeasurements,
                        vedeerSettings.TankId,
                        sunpApiClient,
                        logger,
                        retryCount: 10
                    );
                    _ = processTransfers(
                        result.TransferDtos,
                        collectedTankTransfers,
                        vedeerSettings.TankId,
                        sunpApiClient,
                        logger,
                        retryCount: 10
                    );
                }, TimeSpan.FromSeconds(10));

            } catch (Exception ex) {
                Console.WriteLine($"Unexpected error: {ex.Message}");
                Console.WriteLine($"Unexpected error: {ex.StackTrace}");
            }
        }
    }

    private static async Task processMeasurements(List<SourceValue> measurementData, List<TankMeasurementData> collectedTankMeasurements, long tankId, SunpApiClient sunpApiClient, ILogger logger, int retryCount) {
        if (collectedTankMeasurements.Count() >= 6) {
            logger.LogInformation("Preparing package...");
            var measurementsPackageId = Guid.NewGuid().ToString();
            var measurementsRequestBody = new SendTankIndicatorsRequestBody {
                PackageId = measurementsPackageId,
                RequestGuid = Guid.NewGuid().ToString(),
                TanksMeasurements = new[] {
                    new TankMeasurements {
                        TankId = tankId,
                        Measurements = collectedTankMeasurements
                    }
                }
            };

            var savedFileName = measurementsRequestBody.SaveToFile(logger);
            logger.LogInformation($"Sending package {savedFileName}...");
            var sendMeasurementsResult = await sunpApiClient.TankSendTankIndicatorsAsync(measurementsRequestBody);
            if (sendMeasurementsResult.Success) {
                CommonHelper.DeleteFile(savedFileName, logger);
                collectedTankMeasurements.Clear();
                logger.LogInformation($"Package {savedFileName} sucessfully accepted");
            } else {
                logger.LogError($"Error when sending package {savedFileName}: {sendMeasurementsResult.Error}");
                if (retryCount <= 0) {
                    logger.LogError($"It wasn't possible to send {savedFileName} after. Error message saved in file content");
                    CommonHelper.RenameFileAndAddErrorMessage<SendTankIndicatorsRequestBody>(savedFileName, sendMeasurementsResult.Error, logger);
                } else {
                    await Task.Delay(TimeSpan.FromMinutes(1));
                    logger.LogError($"Retrying to send package {savedFileName}. Retry count: {retryCount}");
                    _ = processMeasurements(measurementData, collectedTankMeasurements, tankId, sunpApiClient, logger, --retryCount);
                }
            }
        } else {
            logger.LogInformation("Collecting measurements...");
            collectedTankMeasurements.AddRange(measurementData.Select(i => new TankMeasurementData {
                MeasurementDate = i.DatetimeStamp ?? DateTime.Now,
                Mass = Convert.ToDecimal(i.Mass),
                Volume = Convert.ToDecimal(i.Volume),
                Level = Convert.ToDecimal(i.Level),
                Density = Convert.ToDecimal(i.Density),
                Temperature = Convert.ToDecimal(i.Temperature)
            }));
            logger.LogInformation("Measurements collected {0}", collectedTankMeasurements.Count());
        }
    }

    private static async Task processTransfers(List<TransferDTO> transferData, List<TankTransferData> collectedTankTransfers, long tankId, SunpApiClient sunpApiClient, ILogger logger, int retryCount) {
        if (collectedTankTransfers.Count() >= 6) {
            logger.LogInformation("Preparing package...");
            var transferDataPackageId = Guid.NewGuid().ToString();
            var transferDataRequestBody = new SendTankTransfersRequestBody {
                PackageId = transferDataPackageId,
                RequestGuid = Guid.NewGuid().ToString(),
                TanksTransfers = new[] {
                    new TankTransfers {
                        TankId = tankId,
                        Transfers = collectedTankTransfers
                    }
                }
            };

            var savedFileName = transferDataRequestBody.SaveToFile(logger);
            logger.LogInformation($"Sending package {savedFileName}...");
            var sendTransferDataResult = await sunpApiClient.TankSendTankTransfersAsync(transferDataRequestBody);
            if (sendTransferDataResult.Success) {
                CommonHelper.DeleteFile(savedFileName, logger);
                collectedTankTransfers.Clear();
                logger.LogInformation($"Package {savedFileName} sucessfully accepted");
            } else {
                logger.LogError($"Error when sending package {savedFileName}: {sendTransferDataResult.Error}");
                if (retryCount <= 0) {
                    logger.LogCritical($"It wasn't possible to send package {savedFileName}. Error message saved in file content");
                    CommonHelper.RenameFileAndAddErrorMessage<SendTankTransfersRequestBody>(savedFileName, sendTransferDataResult.Error, logger);
                } else {
                    await Task.Delay(TimeSpan.FromMinutes(1));
                    logger.LogInformation($"Retrying to send package {savedFileName}. Retry count: {retryCount}");
                    _ = processTransfers(transferData, collectedTankTransfers, tankId, sunpApiClient, logger, --retryCount);
                }
            }
        } else {
            collectedTankTransfers.AddRange(transferData.Select(i => new TankTransferData {
                MeasurementDate = DateTime.Now,
                StartDate = i.StartTime,
                EndDate = i.EndTime,
                Level = Convert.ToDecimal(i.Level),
                Mass = Convert.ToDecimal(i.Mass),
                Volume = Convert.ToDecimal(i.Volume)
            }));
            logger.LogInformation("Transfers collected {0}", collectedTankTransfers.Count());
        }
    }
}