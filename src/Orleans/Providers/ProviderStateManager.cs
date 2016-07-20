using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;
using Orleans.Runtime;

namespace Orleans.Providers
{
    internal enum ProviderState
    {
        None,
        Initialized,
        Started,
        Stopped,
        Closed
    }
    internal class ProviderStateManager
    {
        public ProviderState State { get; private set; }
        private ProviderState presetState;

        public ProviderStateManager()
        {
            State = ProviderState.None;
        }

        public bool PresetState(ProviderState state)
        {
            presetState = state;
            switch (state)
            {
                case ProviderState.None:
                    throw new ProviderStateException("Provider state can not be set to none.");

                case ProviderState.Initialized:
                    switch(State)
                    {
                        case ProviderState.None:
                            return true;
                    }
                    break;

                case ProviderState.Started:
                    switch(State)
                    {
                        case ProviderState.None:
                            throw new ProviderStateException("Trying to start a provider that hasn't been initialized.");
                        case ProviderState.Initialized:
                        case ProviderState.Stopped:
                            return true;
                        case ProviderState.Closed:
                            // Is this correct?
                            throw new ProviderStateException("Trying to start a provider that has been closed.");
                    }
                    break;

                case ProviderState.Stopped:
                    switch (State)
                    {
                        case ProviderState.None:
                        case ProviderState.Initialized:
                            throw new ProviderStateException("Trying to stop a provider that hasn't been started. Current state:" + State.ToString());
                        case ProviderState.Started:
                            return true;
                    }
                    break;

                case ProviderState.Closed:
                    // TODO: What's the action supposed to be here?
                    return true;
            }

            return false;
        }

        public void CommitState()
        {
            State = presetState;
        }
    }

    [Serializable]
    public class ProviderStateException : OrleansException
    {
        public ProviderStateException() : base("Unexpected provider state")
        { }
        public ProviderStateException(string message) : base(message) { }

        public ProviderStateException(string message, Exception innerException) : base(message, innerException) { }

        protected ProviderStateException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }
    }

}
