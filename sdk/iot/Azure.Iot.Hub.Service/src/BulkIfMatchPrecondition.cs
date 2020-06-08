// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

namespace Azure.Iot.Hub.Service
{
    /// <summary>
    /// The condition on which each operation in a bulk operation will execute.
    /// </summary>
    public enum BulkIfMatchPrecondition
    {
        /// <summary>
        /// Perform this operation regardless of if the provided resource matches the service's representation
        /// of the object.
        /// </summary>
        Unconditional,

        /// <summary>
        /// Perform this operation only if the resource exists in the service, and the provided entity's ETag
        /// matches the service's ETag.
        /// </summary>
        ifMatch
    }
}
