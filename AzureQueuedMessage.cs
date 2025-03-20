using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Azure;
using Azure.Storage.Queues;
using Azure.Storage.Queues.Models;

namespace Grammophone.Queueing.Azure
{
	/// <summary>
	/// Implementation of <see cref="IQueuedMessage"/> for Azure Storage Queues.
	/// </summary>
	public class AzureQueuedMessage : IQueuedMessage
	{
		#region Private fields
		
		private readonly QueueMessage underlyingMessage;
		
		private readonly QueueClient underlyingClient;

		#endregion

		#region Construction

		internal AzureQueuedMessage(QueueMessage underlyingMessage, QueueClient underlyingClient)
		{
			if (underlyingMessage == null) throw new ArgumentNullException(nameof(underlyingMessage));
			if (underlyingClient == null) throw new ArgumentNullException(nameof(underlyingClient));

			this.underlyingMessage = underlyingMessage;
			this.underlyingClient = underlyingClient;
		}

		#endregion

		#region IQueuedMessage implementation

		/// <inheritdoc/>
		public BinaryData Body => underlyingMessage.Body;

		/// <inheritdoc/>
		public async Task<bool> TryAbandonAsync(CancellationToken cancellationToken = default)
		{
			try
			{
				var response = await underlyingClient.UpdateMessageAsync(
					underlyingMessage.MessageId, underlyingMessage.PopReceipt, visibilityTimeout: TimeSpan.Zero, cancellationToken: cancellationToken);

				return true;
			}
			catch (RequestFailedException ex) when (ex.Status == (int)HttpStatusCode.BadRequest || ex.Status == (int)HttpStatusCode.NotFound)
			{
				return false;
			}
		}

		/// <inheritdoc/>
		public async Task<bool> TryCommitAsync(CancellationToken cancellationToken = default)
		{
			try
			{
				await underlyingClient.DeleteMessageAsync(underlyingMessage.MessageId, underlyingMessage.PopReceipt, cancellationToken);

				return true;
			}
			catch (RequestFailedException ex) when (ex.Status == (int)HttpStatusCode.BadRequest || ex.Status == (int)HttpStatusCode.NotFound)
			{
				return false;
			}

		}

		#endregion
	}
}
