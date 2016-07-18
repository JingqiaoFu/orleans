using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans.Runtime.Configuration;

namespace Orleans.Runtime
{
    /// <summary>
    /// IStreamProviderUpdateAgent interface that defines interface for runtime adding/removing stream providers.
    /// </summary>
    internal interface IStreamProviderUpdateAgent : ISystemTarget
    {
        Task UpdateStreamProviders(IDictionary<string, ProviderCategoryConfiguration> streamProviderConfigurations);
    }
}
