using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Azure.Storage.Queues;

namespace Grammophone.Queueing.Azure
{
	/// <summary>
	/// Implementation of <see cref="IQueueingClient"/> for Azure Storage Queues.
	/// </summary>
	public class AzureQueueingClient : IQueueingClient
	{
		#region Private fields

		private readonly QueueClient underlyingClient;

		#endregion

		#region Construction

		internal AzureQueueingClient(QueueClient underlyingClient, TimeSpan visibilityTimeout, TimeSpan timeToLive)
		{
			if (underlyingClient == null) throw new ArgumentNullException(nameof(underlyingClient));

			this.underlyingClient = underlyingClient;
			this.VisibilityTimeout = visibilityTimeout;
			this.TimeToLive = timeToLive;
		}

		#endregion

		#region Public properties

		/// <summary>
		/// Visibility timeout. Cannot be larger than 7 days.
		/// </summary>
		public TimeSpan VisibilityTimeout { get; }

		/// <summary>
		/// Specifies the time-to-live interval for the message
		/// </summary>
		public TimeSpan TimeToLive { get; }

		#endregion

		#region IQueueingClient implementation

		/// <inheritdoc/>
		public Task SendMessageAsync(byte[] data, CancellationToken cancellationToken = default)
			=> SendMessageAsync(new BinaryData(data), cancellationToken);

		/// <inheritdoc/>
		public Task SendMessageAsync(string data, CancellationToken cancellationToken = default)
			=> underlyingClient.SendMessageAsync(data, TimeSpan.Zero, this.TimeToLive, cancellationToken);

		/// <inheritdoc/>
		public Task SendMessageAsync(BinaryData data, CancellationToken cancellationToken = default)
			=> underlyingClient.SendMessageAsync(data, TimeSpan.Zero, this.TimeToLive, cancellationToken);

		/// <inheritdoc/>
		public async Task<IQueuedMessage?> TryReceiveMessageAsync(CancellationToken cancellationToken = default)
		{
			var underlyingMessageResponse = await underlyingClient.ReceiveMessageAsync(this.VisibilityTimeout, cancellationToken);

			var underlyingMessage = underlyingMessageResponse.Value;

			if (underlyingMessage == null) return null;

			return new AzureQueuedMessage(underlyingMessage, underlyingClient);
		}

		#endregion
	}
}
