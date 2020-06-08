// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Azure.Core;
using Azure.Iot.Hub.Service.Models;

namespace Azure.Iot.Hub.Service
{
    /// <summary>
    /// Device Client place holder
    /// </summary>
    public class DevicesClient
    {
        private const string ContinuationTokenHeader = "x-ms-continuation";

        private readonly RegistryManagerRestClient _registryManagerClient;
        private readonly TwinRestClient _twinClient;
        private readonly DeviceMethodRestClient _deviceMethodClient;

        protected DevicesClient()
        {
        }

        internal DevicesClient(RegistryManagerRestClient registryManagerClient, TwinRestClient twinRestClient, DeviceMethodRestClient deviceMethodRestClient)
        {
            Argument.AssertNotNull(registryManagerClient, nameof(registryManagerClient));
            Argument.AssertNotNull(twinRestClient, nameof(twinRestClient));
            Argument.AssertNotNull(deviceMethodRestClient, nameof(deviceMethodRestClient));

            _registryManagerClient = registryManagerClient;
            _twinClient = twinRestClient;
            _deviceMethodClient = deviceMethodRestClient;
        }

        /// <summary>
        /// Create or update a device.
        /// </summary>
        /// <param name="deviceIdentity">The device to create.</param>
        /// <param name="precondition">The condition on which to perform this operation. To create a device identity, this value must be equal to <see cref="IfMatchPrecondition.Unconditional"/>.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The created device.</returns>
        public virtual async Task<Response<DeviceIdentity>> CreateOrUpdateIdentityAsync(DeviceIdentity deviceIdentity, IfMatchPrecondition precondition, CancellationToken cancellationToken = default)
        {
            Argument.AssertNotNull(deviceIdentity, nameof(deviceIdentity));
            String ifMatchHeaderValue = IfMatchPreconditionExtensions.getIfMatchHeaderValue(precondition, deviceIdentity.Etag);
            return await _registryManagerClient.CreateOrUpdateDeviceAsync(deviceIdentity.DeviceId, deviceIdentity, ifMatchHeaderValue, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Create or update a device.
        /// </summary>
        /// <param name="deviceIdentity">The device to create.</param>
        /// <param name="precondition">The condition on which to perform this operation. To create a device identity, this value must be equal to <see cref="IfMatchPrecondition.Unconditional"/>.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The created device.</returns>
        public virtual Response<DeviceIdentity> CreateOrUpdateIdentity(DeviceIdentity deviceIdentity, IfMatchPrecondition precondition, CancellationToken cancellationToken = default)
        {
            Argument.AssertNotNull(deviceIdentity, nameof(deviceIdentity));
            String ifMatchHeaderValue = IfMatchPreconditionExtensions.getIfMatchHeaderValue(precondition, deviceIdentity.Etag);
            return _registryManagerClient.CreateOrUpdateDevice(deviceIdentity.DeviceId, deviceIdentity, ifMatchHeaderValue, cancellationToken);
        }

        /// <summary>
        /// Get a single device.
        /// </summary>
        /// <param name="deviceId">The unique identifier of the device to get.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The retrieved device.</returns>
        public virtual async Task<Response<DeviceIdentity>> GetIdentityAsync(string deviceId, CancellationToken cancellationToken = default)
        {
            return await _registryManagerClient.GetDeviceAsync(deviceId, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Get a single device.
        /// </summary>
        /// <param name="deviceId">The unique identifier of the device to get.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The retrieved device.</returns>
        public virtual Response<DeviceIdentity> GetIdentity(string deviceId, CancellationToken cancellationToken = default)
        {
            return _registryManagerClient.GetDevice(deviceId, cancellationToken);
        }

        /// <summary>
        /// Delete a single device.
        /// </summary>
        /// <param name="deviceIdentity">The device to delete. If no ETag is present on the device, then the condition must be equal to <see cref="IfMatchPrecondition.Unconditional"/> or equal to <see cref="IfMatchPrecondition.UnconditionalIfMatch"/></param>
        /// <param name="precondition">The condition on which to delete the device.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The http response.</returns>
        public virtual async Task<Response> DeleteIdentityAsync(DeviceIdentity deviceIdentity, IfMatchPrecondition precondition, CancellationToken cancellationToken = default)
        {
            Argument.AssertNotNull(deviceIdentity, nameof(deviceIdentity));
            String ifMatchHeaderValue = IfMatchPreconditionExtensions.getIfMatchHeaderValue(precondition, deviceIdentity.Etag);
            return await _registryManagerClient.DeleteDeviceAsync(deviceIdentity.DeviceId, ifMatchHeaderValue, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Delete a single device.
        /// </summary>
        /// <param name="deviceIdentity">The device to delete. If no ETag is present on the device, then the condition must be equal to <see cref="IfMatchPrecondition.Unconditional"/> or equal to <see cref="IfMatchPrecondition.UnconditionalIfMatch"/></param>
        /// <param name="precondition">The condition on which to delete the device.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The http response.</returns>
        public virtual Response DeleteIdentity(DeviceIdentity deviceIdentity, IfMatchPrecondition precondition, CancellationToken cancellationToken = default)
        {
            Argument.AssertNotNull(deviceIdentity, nameof(deviceIdentity));
            String ifMatchHeaderValue = IfMatchPreconditionExtensions.getIfMatchHeaderValue(precondition, deviceIdentity.Etag);
            return _registryManagerClient.DeleteDevice(deviceIdentity.DeviceId, ifMatchHeaderValue, cancellationToken);
        }

        /// <summary>
        /// Create multiple devices with an initial twin. A maximum of 100 creations can be done per call, and each creation must have a unique device identity. For larger scale operations, consider using IoT Hub jobs (https://docs.microsoft.com/en-us/azure/iot-hub/iot-hub-devguide-identity-registry#import-and-export-device-identities).
        /// </summary>
        /// <param name="devices">The pairs of devices their twins that will be created. For fields such as deviceId
        /// where device and twin have a definition, the device value will override the twin value.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The result of the bulk operation.</returns>
        public virtual async Task<Response<BulkRegistryOperationResult>> CreateIdentitiesWithTwinAsync(IDictionary<DeviceIdentity, TwinData> devices, CancellationToken cancellationToken = default)
        {
            IEnumerable<ExportImportDevice> registryOperations = devices
                .ToList()
                .Select(x => new ExportImportDevice()
                {
                    Id = x.Key.DeviceId,
                    Authentication = x.Key.Authentication,
                    Capabilities = x.Key.Capabilities,
                    DeviceScope = x.Key.DeviceScope,
                    ParentScopes = x.Key.ParentScopes,
                    Status = string.Equals(ExportImportDeviceStatus.Disabled.ToString(), x.Key.Status?.ToString(), StringComparison.OrdinalIgnoreCase) ? ExportImportDeviceStatus.Disabled : ExportImportDeviceStatus.Enabled,
                    StatusReason = x.Key.StatusReason,
                    Tags = x.Value.Tags,
                    Properties = new PropertyContainer(x.Value.Properties?.Desired, x.Value.Properties?.Reported),
                    ImportMode = ExportImportDeviceImportMode.Create
                });

            return await _registryManagerClient.BulkDeviceCrudAsync(registryOperations, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Create multiple devices with an initial twin. A maximum of 100 creations can be done per call, and each creation must have a unique device identity. For larger scale operations, consider using IoT Hub jobs (https://docs.microsoft.com/en-us/azure/iot-hub/iot-hub-devguide-identity-registry#import-and-export-device-identities).
        /// </summary>
        /// <param name="devices">The pairs of devices their twins that will be created. For fields such as deviceId
        /// where device and twin have a definition, the device value will override the twin value.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The result of the bulk operation.</returns>
        public virtual Response<BulkRegistryOperationResult> CreateIdentitiesWithTwin(IDictionary<DeviceIdentity, TwinData> devices, CancellationToken cancellationToken = default)
        {
            IEnumerable<ExportImportDevice> registryOperations = devices
                .ToList()
                .Select(x => new ExportImportDevice()
                {
                    Id = x.Key.DeviceId,
                    Authentication = x.Key.Authentication,
                    Capabilities = x.Key.Capabilities,
                    DeviceScope = x.Key.DeviceScope,
                    ParentScopes = x.Key.ParentScopes,
                    Status = string.Equals(ExportImportDeviceStatus.Disabled.ToString(), x.Key.Status?.ToString(), StringComparison.OrdinalIgnoreCase) ? ExportImportDeviceStatus.Disabled : ExportImportDeviceStatus.Enabled,
                    StatusReason = x.Key.StatusReason,
                    Tags = x.Value.Tags,
                    Properties = new PropertyContainer(x.Value.Properties?.Desired, x.Value.Properties?.Reported),
                    ImportMode = ExportImportDeviceImportMode.Create
                });

            return _registryManagerClient.BulkDeviceCrud(registryOperations, cancellationToken);
        }

        /// <summary>
        /// Create multiple devices. A maximum of 100 creations can be done per call, and each device identity must be unique. For larger scale operations, consider using IoT Hub jobs (https://docs.microsoft.com/en-us/azure/iot-hub/iot-hub-devguide-identity-registry#import-and-export-device-identities).
        /// </summary>
        /// <param name="deviceIdentities">The devices to create.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The result of the bulk operation.</returns>
        public virtual async Task<Response<BulkRegistryOperationResult>> CreateIdentitiesAsync(IEnumerable<DeviceIdentity> deviceIdentities, CancellationToken cancellationToken = default)
        {
            IEnumerable<ExportImportDevice> registryOperations = deviceIdentities
                .ToList()
                .Select(x => new ExportImportDevice()
                {
                    Id = x.DeviceId,
                    Authentication = x.Authentication,
                    Capabilities = x.Capabilities,
                    DeviceScope = x.DeviceScope,
                    ParentScopes = x.ParentScopes,
                    Status = string.Equals(ExportImportDeviceStatus.Disabled.ToString(), x.Status?.ToString(), StringComparison.OrdinalIgnoreCase) ? ExportImportDeviceStatus.Disabled : ExportImportDeviceStatus.Enabled,
                    StatusReason = x.StatusReason,
                    ImportMode = ExportImportDeviceImportMode.Create
                });

            return await _registryManagerClient.BulkDeviceCrudAsync(registryOperations, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Create multiple devices. A maximum of 100 creations can be done per call, and each device identity must be unique. For larger scale operations, consider using IoT Hub jobs (https://docs.microsoft.com/en-us/azure/iot-hub/iot-hub-devguide-identity-registry#import-and-export-device-identities).
        /// </summary>
        /// <param name="deviceIdentities">The devices to create.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The result of the bulk operation.</returns>
        public virtual Response<BulkRegistryOperationResult> CreateIdentities(IEnumerable<DeviceIdentity> deviceIdentities, CancellationToken cancellationToken = default)
        {
            IEnumerable<ExportImportDevice> registryOperations = deviceIdentities
                .ToList()
                .Select(x => new ExportImportDevice()
                {
                    Id = x.DeviceId,
                    Authentication = x.Authentication,
                    Capabilities = x.Capabilities,
                    DeviceScope = x.DeviceScope,
                    ParentScopes = x.ParentScopes,
                    Status = string.Equals(ExportImportDeviceStatus.Disabled.ToString(), x.Status?.ToString(), StringComparison.OrdinalIgnoreCase) ? ExportImportDeviceStatus.Disabled : ExportImportDeviceStatus.Enabled,
                    StatusReason = x.StatusReason,
                    ImportMode = ExportImportDeviceImportMode.Create
                });

            return _registryManagerClient.BulkDeviceCrud(registryOperations, cancellationToken);
        }

        /// <summary>
        /// Update multiple devices. A maximum of 100 updates can be done per call, and each operation must be done on a different identity. For larger scale operations, consider using IoT Hub jobs (https://docs.microsoft.com/en-us/azure/iot-hub/iot-hub-devguide-identity-registry#import-and-export-device-identities).
        /// </summary>
        /// <param name="deviceIdentities">The devices to update.</param>
        /// <param name="precondition">Perform these operations only on this condition.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The result of the bulk operation.</returns>
        public virtual async Task<Response<BulkRegistryOperationResult>> UpdateIdentiesAsync(IEnumerable<DeviceIdentity> deviceIdentities, BulkIfMatchPrecondition precondition, CancellationToken cancellationToken = default)
        {
            IEnumerable<ExportImportDevice> registryOperations = deviceIdentities
                .ToList()
                .Select(x => new ExportImportDevice()
                {
                    Id = x.DeviceId,
                    Authentication = x.Authentication,
                    Capabilities = x.Capabilities,
                    DeviceScope = x.DeviceScope,
                    ParentScopes = x.ParentScopes,
                    ETag = x.Etag,
                    Status = string.Equals(ExportImportDeviceStatus.Disabled.ToString(), x.Status?.ToString(), StringComparison.OrdinalIgnoreCase) ? ExportImportDeviceStatus.Disabled : ExportImportDeviceStatus.Enabled,
                    StatusReason = x.StatusReason,
                    ImportMode = precondition == BulkIfMatchPrecondition.Unconditional ? ExportImportDeviceImportMode.Update : ExportImportDeviceImportMode.UpdateIfMatchETag
                });

            return await _registryManagerClient.BulkDeviceCrudAsync(registryOperations, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Update multiple devices. A maximum of 100 updates can be done per call, and each operation must be done on a different identity. For larger scale operations, consider using IoT Hub jobs (https://docs.microsoft.com/en-us/azure/iot-hub/iot-hub-devguide-identity-registry#import-and-export-device-identities).
        /// </summary>
        /// <param name="deviceIdentities">The devices to update.</param>
        /// <param name="precondition">The condition for which each operation will execute.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The result of the bulk operation.</returns>
        public virtual Response<BulkRegistryOperationResult> UpdateIdenties(IEnumerable<DeviceIdentity> deviceIdentities, BulkIfMatchPrecondition precondition, CancellationToken cancellationToken = default)
        {
            IEnumerable<ExportImportDevice> registryOperations = deviceIdentities
                .ToList()
                .Select(x => new ExportImportDevice()
                {
                    Id = x.DeviceId,
                    Authentication = x.Authentication,
                    Capabilities = x.Capabilities,
                    DeviceScope = x.DeviceScope,
                    ParentScopes = x.ParentScopes,
                    ETag = x.Etag,
                    Status = string.Equals(ExportImportDeviceStatus.Disabled.ToString(), x.Status?.ToString(), StringComparison.OrdinalIgnoreCase) ? ExportImportDeviceStatus.Disabled : ExportImportDeviceStatus.Enabled,
                    StatusReason = x.StatusReason,
                    ImportMode = precondition == BulkIfMatchPrecondition.Unconditional ? ExportImportDeviceImportMode.Update : ExportImportDeviceImportMode.UpdateIfMatchETag
                });

            return _registryManagerClient.BulkDeviceCrud(registryOperations, cancellationToken);
        }

        /// <summary>
        /// Delete multiple devices. A maximum of 100 deletions can be done per call. For larger scale operations, consider using IoT Hub jobs (https://docs.microsoft.com/en-us/azure/iot-hub/iot-hub-devguide-identity-registry#import-and-export-device-identities).
        /// </summary>
        /// <param name="deviceIdentities">The devices to delete.</param>
        /// <param name="precondition">The condition for which each operation will execute.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The result of the bulk deletion.</returns>
        public virtual async Task<Response<BulkRegistryOperationResult>> DeleteIdentitiesAsync(IEnumerable<DeviceIdentity> deviceIdentities, BulkIfMatchPrecondition precondition, CancellationToken cancellationToken = default)
        {
            IEnumerable<ExportImportDevice> registryOperations = deviceIdentities
                .ToList()
                .Select(x => new ExportImportDevice()
                {
                    Id = x.DeviceId,
                    ETag = x.Etag,
                    ImportMode = precondition == BulkIfMatchPrecondition.Unconditional ? ExportImportDeviceImportMode.Delete : ExportImportDeviceImportMode.DeleteIfMatchETag
                });

            return await _registryManagerClient.BulkDeviceCrudAsync(registryOperations, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Delete multiple devices. A maximum of 100 deletions can be done per call. For larger scale operations, consider using IoT Hub jobs (https://docs.microsoft.com/en-us/azure/iot-hub/iot-hub-devguide-identity-registry#import-and-export-device-identities).
        /// </summary>
        /// <param name="deviceIdentities">The devices to delete.</param>
        /// <param name="precondition">The condition for which each operation will execute.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The result of the bulk deletion.</returns>
        public virtual Response<BulkRegistryOperationResult> DeleteIdentities(IEnumerable<DeviceIdentity> deviceIdentities, BulkIfMatchPrecondition precondition, CancellationToken cancellationToken = default)
        {
            IEnumerable<ExportImportDevice> registryOperations = deviceIdentities
                .ToList()
                .Select(x => new ExportImportDevice()
                {
                    Id = x.DeviceId,
                    ETag = x.Etag,
                    ImportMode = precondition == BulkIfMatchPrecondition.Unconditional ? ExportImportDeviceImportMode.Delete : ExportImportDeviceImportMode.DeleteIfMatchETag
                });

            return _registryManagerClient.BulkDeviceCrud(registryOperations, cancellationToken);
        }

        /// <summary>
        /// List a set of device twins.
        /// </summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A pageable set of device twins.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Globalization", "CA1305:Specify IFormatProvider", Justification = "<Pending>")]
        public virtual AsyncPageable<TwinData> GetTwinsAsync(CancellationToken cancellationToken = default)
        {
            async Task<Page<TwinData>> FirstPageFunc(int? pageSizeHint)
            {
                var querySpecification = new QuerySpecification
                {
                    Query = "select * from devices"
                };
                Response<IReadOnlyList<TwinData>> response = await _registryManagerClient.QueryIotHubAsync(querySpecification, null, pageSizeHint?.ToString(), cancellationToken).ConfigureAwait(false);
                response.GetRawResponse().Headers.TryGetValue(ContinuationTokenHeader, out string continuationToken);

                return Page.FromValues(response.Value, continuationToken, response.GetRawResponse());
            }

            async Task<Page<TwinData>> NextPageFunc(string nextLink, int? pageSizeHint)
            {
                var querySpecification = new QuerySpecification();
                Response<IReadOnlyList<TwinData>> response = await _registryManagerClient.QueryIotHubAsync(querySpecification, nextLink, pageSizeHint?.ToString(), cancellationToken).ConfigureAwait(false);
                response.GetRawResponse().Headers.TryGetValue(ContinuationTokenHeader, out string continuationToken);
                return Page.FromValues(response.Value, continuationToken, response.GetRawResponse());
            }

            return PageableHelpers.CreateAsyncEnumerable(FirstPageFunc, NextPageFunc);
        }

        /// <summary>
        /// List a set of device twins.
        /// </summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A pageable set of device twins.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Globalization", "CA1305:Specify IFormatProvider", Justification = "<Pending>")]
        public virtual Pageable<TwinData> GetTwins(CancellationToken cancellationToken = default)
        {
            Page<TwinData> FirstPageFunc(int? pageSizeHint)
            {
                var querySpecification = new QuerySpecification
                {
                    Query = "select * from devices"
                };

                Response<IReadOnlyList<TwinData>> response = _registryManagerClient.QueryIotHub(querySpecification, null, pageSizeHint?.ToString(), cancellationToken);

                response.GetRawResponse().Headers.TryGetValue(ContinuationTokenHeader, out string continuationToken);

                return Page.FromValues(response.Value, continuationToken, response.GetRawResponse());
            }

            Page<TwinData> NextPageFunc(string nextLink, int? pageSizeHint)
            {
                var querySpecification = new QuerySpecification();
                Response<IReadOnlyList<TwinData>> response = _registryManagerClient.QueryIotHub(querySpecification, nextLink, pageSizeHint?.ToString(), cancellationToken);
                response.GetRawResponse().Headers.TryGetValue(ContinuationTokenHeader, out string continuationToken);
                return Page.FromValues(response.Value, continuationToken, response.GetRawResponse());
            }

            return PageableHelpers.CreateEnumerable(FirstPageFunc, NextPageFunc);
        }

        /// <summary>
        /// Get a device's twin.
        /// </summary>
        /// <param name="deviceId">The unique identifier of the device to get the twin of.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The device's twin, including reported properties and desired properties.</returns>
        public virtual async Task<Response<TwinData>> GetTwinAsync(string deviceId, CancellationToken cancellationToken = default)
        {
            return await _twinClient.GetDeviceTwinAsync(deviceId, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Get a device's twin.
        /// </summary>
        /// <param name="deviceId">The unique identifier of the device to get the twin of.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The device's twin, including reported properties and desired properties.</returns>
        public virtual Response<TwinData> GetTwin(string deviceId, CancellationToken cancellationToken = default)
        {
            return _twinClient.GetDeviceTwin(deviceId, cancellationToken);
        }

        /// <summary>
        /// Update a device's twin.
        /// </summary>
        /// <param name="twinPatch">The properties to update. Any existing properties not referenced by this patch will be unaffected by this patch.</param>
        /// <param name="precondition">The condition for which this operation will execute.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The server's new representation of the device twin.</returns>
        public virtual async Task<Response<TwinData>> UpdateTwinAsync(TwinData twinPatch, IfMatchPrecondition precondition, CancellationToken cancellationToken = default)
        {
            Argument.AssertNotNull(twinPatch, nameof(twinPatch));
            String ifMatchHeaderValue = IfMatchPreconditionExtensions.getIfMatchHeaderValue(precondition, twinPatch.Etag);
            return await _twinClient.UpdateDeviceTwinAsync(twinPatch.DeviceId, twinPatch, ifMatchHeaderValue, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Update a device's twin.
        /// </summary>
        /// <param name="twinPatch">The properties to update. Any existing properties not referenced by this patch will be unaffected by this patch.</param>
        /// <param name="precondition">The condition for which this operation will execute.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The server's new representation of the device twin.</returns>
        public virtual Response<TwinData> UpdateTwin(TwinData twinPatch, IfMatchPrecondition precondition, CancellationToken cancellationToken = default)
        {
            Argument.AssertNotNull(twinPatch, nameof(twinPatch));
            String ifMatchHeaderValue = IfMatchPreconditionExtensions.getIfMatchHeaderValue(precondition, twinPatch.Etag);
            return _twinClient.UpdateDeviceTwin(twinPatch.DeviceId, twinPatch, ifMatchHeaderValue, cancellationToken);
        }

        /// <summary>
        /// Update multiple devices' twins. A maximum of 100 updates can be done per call, and each operation must be done on a different device twin. For larger scale operations, consider using IoT Hub jobs (https://docs.microsoft.com/en-us/azure/iot-hub/iot-hub-devguide-identity-registry#import-and-export-device-identities).
        /// </summary>
        /// <param name="twinUpdates">The new twins to replace the twins on existing devices.</param>
        /// <param name="precondition">The condition for which each operation will execute.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The result of the bulk operation.</returns>
        public virtual async Task<Response<BulkRegistryOperationResult>> UpdateTwinsAsync(IEnumerable<TwinData> twinUpdates, BulkIfMatchPrecondition precondition, CancellationToken cancellationToken = default)
        {
            IEnumerable<ExportImportDevice> registryOperations = twinUpdates
                .ToList()
                .Select(x => new ExportImportDevice()
                {
                    Id = x.DeviceId,
                    Tags = x.Tags,
                    Properties = new PropertyContainer(x.Properties?.Desired, x.Properties?.Reported),
                    TwinETag = x.Etag,
                    ImportMode = precondition == BulkIfMatchPrecondition.Unconditional ? ExportImportDeviceImportMode.UpdateTwin : ExportImportDeviceImportMode.UpdateTwinIfMatchETag
                });

            return await _registryManagerClient.BulkDeviceCrudAsync(registryOperations, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Update multiple devices' twins. A maximum of 100 updates can be done per call, and each operation must be done on a different device twin. For larger scale operations, consider using IoT Hub jobs (https://docs.microsoft.com/en-us/azure/iot-hub/iot-hub-devguide-identity-registry#import-and-export-device-identities).
        /// </summary>
        /// <param name="twinUpdates">The new twins to replace the twins on existing devices.</param>
        /// <param name="precondition">The condition for which each operation will execute.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The result of the bulk operation.</returns>
        public virtual Response<BulkRegistryOperationResult> UpdateTwins(IEnumerable<TwinData> twinUpdates, BulkIfMatchPrecondition precondition, CancellationToken cancellationToken = default)
        {
            IEnumerable<ExportImportDevice> registryOperations = twinUpdates
                .ToList()
                .Select(x => new ExportImportDevice()
                {
                    Id = x.DeviceId,
                    Tags = x.Tags,
                    Properties = new PropertyContainer(x.Properties?.Desired, x.Properties?.Reported),
                    TwinETag = x.Etag,
                    ImportMode = precondition == BulkIfMatchPrecondition.Unconditional ? ExportImportDeviceImportMode.UpdateTwin : ExportImportDeviceImportMode.UpdateTwinIfMatchETag
                });

            return _registryManagerClient.BulkDeviceCrud(registryOperations, cancellationToken);
        }

        /// <summary>
        /// Invoke a method on a device.
        /// </summary>
        /// <param name="deviceId">The unique identifier of the device to invoke the method on.</param>
        /// <param name="directMethodRequest">The details of the method to invoke.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The result of the method invocation.</returns>
        public virtual async Task<Response<CloudToDeviceMethodResponse>> InvokeMethodAsync(string deviceId, CloudToDeviceMethodRequest directMethodRequest, CancellationToken cancellationToken = default)
        {
            return await _deviceMethodClient.InvokeDeviceMethodAsync(deviceId, directMethodRequest, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Invoke a method on a device.
        /// </summary>
        /// <param name="deviceId">The unique identifier of the device to invoke the method on.</param>
        /// <param name="directMethodRequest">The details of the method to invoke.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The result of the method invocation.</returns>
        public virtual Response<CloudToDeviceMethodResponse> InvokeMethod(string deviceId, CloudToDeviceMethodRequest directMethodRequest, CancellationToken cancellationToken = default)
        {
            return _deviceMethodClient.InvokeDeviceMethod(deviceId, directMethodRequest, cancellationToken);
        }
    }
}
