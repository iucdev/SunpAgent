using System.Globalization;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Sunp.Api.Client;

namespace SunpAgent.Helpers {
    public static class CommonHelper {
        public static decimal TryParseDecimal(string valAsString) {
            try {
                return Convert.ToDecimal(valAsString, CultureInfo.InvariantCulture);
            } catch (Exception ex) {
                throw new Exception($"It wasn't possible to convert string {valAsString} to decimal");
            }
        }
        
        public static decimal TryParseDecimal(double valAsDouble) {
            try {
                return Convert.ToDecimal(valAsDouble, CultureInfo.InvariantCulture);
            } catch (Exception ex) {
                throw new Exception($"It wasn't possible to convert double {valAsDouble} to decimal");
            }
        }

        public static string SaveToFile(this SendTankIndicatorsRequestBody tankIndicatorsRequestBody, ILogger logger) {
            try {
                var json = JsonConvert.SerializeObject(tankIndicatorsRequestBody);
                var fileName = $"TanksIndicators_{tankIndicatorsRequestBody.PackageId}.json";
                File.WriteAllText(fileName, json);
                return fileName;
            } catch (Exception ex) {
                logger.LogError("Error on SendTankIndicatorsRequestBody file saving: {0}", ex.StackTrace);
                throw;
            }
        }

        public static string SaveToFile(this SendTankTransfersRequestBody tankTransfersRequestBody, ILogger logger) {
            try {
                var json = JsonConvert.SerializeObject(tankTransfersRequestBody);
                var fileName = $"TanksTransfers_{tankTransfersRequestBody.PackageId}.json";
                File.WriteAllText(fileName, json);
                return fileName;
            } catch (Exception ex) { 
                logger.LogError("Error on SendTankTransfersRequestBody file saving: {0}", ex.StackTrace);
                throw;
            }
        }

        public static void DeleteFile(string fileName, ILogger logger) {
            try {
                if (File.Exists(fileName)) {
                    File.Delete(fileName);
                } else {
                }
            } catch (Exception ex) {
                logger.LogError($"Error deleting file {fileName}: {ex.Message}");
            }
        }

        public static void RenameFileAndAddErrorMessage<T>(string fileName, string additionalText, ILogger logger) {
            try {
                if (File.Exists(fileName)) {
                    var directory = Path.GetDirectoryName(fileName);
                    var newFileName = Path.Combine(directory, "Error_" + Path.GetFileName(fileName));
                    var json = File.ReadAllText(fileName);
                    var jsonDocument = JsonDocument.Parse(json);
                    var jsonObject = JsonConvert.DeserializeObject<T>(json);
                    var jsonObjectWithText = new {
                        originalData = jsonObject,
                        errorMessage = additionalText
                    };
                    var updatedJson = JsonConvert.SerializeObject(jsonObjectWithText);
                    File.Move(fileName, newFileName);
                    File.WriteAllText(newFileName, updatedJson);
                }
            } catch (Exception ex) {
                logger.LogError($"Error renaming file {fileName} or adding error text: {ex.Message}");
            }
        }
    }
}
