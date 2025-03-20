using System;
using Azure.Storage.Queues;

namespace Grammophone.Queueing.Azure
{
	/// <summary>
	/// <see cref="IQueueingProvider"/> implementation for Azure Storage Queues.
	/// It is thread-safe as required by the contract.
	/// </summary>
	public class AzureQueueingProvider : IQueueingProvider
	{
		#region Private fields

		private readonly QueueClientOptions underlyingQueueClientOptions;;

		#endregion

		#region Construction

		/// <summary>
		/// Create.
		/// </summary>
		/// <param name="connectionString">The connection string to the Azure Storage account.</param>
		/// <param name="queueName"> The name of the queue.</param>
		/// <param name="visibilityTimeout">Visibility timeout. Cannot be larger than 7 days.</param>
		/// <param name="timeToLive">Specifies the time-to-live interval for the message</param>
		public AzureQueueingProvider(string connectionString, string queueName, TimeSpan visibilityTimeout, TimeSpan timeToLive)
		{
			if (connectionString == null) throw new ArgumentNullException(nameof(connectionString));
			if (queueName == null) throw new ArgumentNullException(nameof(queueName));

			this.ConnectionString = connectionString;
			this.QueueName = queueName;
			this.VisibilityTimeout = visibilityTimeout;
			this.TimeToLive = timeToLive;

			this.underlyingQueueClientOptions = new QueueClientOptions();
		}

		/// <summary>
		/// Create.
		/// </summary>
		/// <param name="connectionString">The connection string to the Azure Storage account.</param>
		/// <param name="queueName"> The name of the queue.</param>
		/// <param name="visibilityTimeout">Visibility timeout. Cannot be larger than 7 days.</param>
		/// <param name="timeToLive">Specifies the time-to-live interval for the message</param>
		/// <param name="serviceVersion">The API version of the Azure service.</param>
		public AzureQueueingProvider(string connectionString, string queueName, TimeSpan visibilityTimeout, TimeSpan timeToLive, QueueClientOptions.ServiceVersion serviceVersion)
		{
			if (connectionString == null) throw new ArgumentNullException(nameof(connectionString));
			if (queueName == null) throw new ArgumentNullException(nameof(queueName));

			this.ConnectionString = connectionString;
			this.QueueName = queueName;
			this.VisibilityTimeout = visibilityTimeout;
			this.TimeToLive = timeToLive;

			this.underlyingQueueClientOptions = new QueueClientOptions(serviceVersion);
		}

		#endregion

		#region Public properties

		/// <summary>
		/// The connection string to the Azure Storage account.
		/// </summary>
		public string ConnectionString { get; }

		/// <summary>
		/// The name of the queue.
		/// </summary>
		public string QueueName { get; }

		/// <summary>
		/// Visibility timeout. Cannot be larger than 7 days.
		/// </summary>
		public TimeSpan VisibilityTimeout { get; }

		/// <summary>
		/// Specifies the time-to-live interval for the message
		/// </summary>
		public TimeSpan TimeToLive { get; }

		#endregion

		#region IQueueingProvider Implementation

		/// <summary>
		/// Creates a client based on the supplied parameters in the constructor of <see cref="AzureQueueingProvider"/>.
		/// </summary>
		public IQueueingClient CreateClient()
		{
			var underlyingClient = new QueueClient(this.ConnectionString, this.QueueName, underlyingQueueClientOptions);

			return new AzureQueueingClient(underlyingClient, this.VisibilityTimeout, this.TimeToLive);
		}

		#endregion
	}
}
