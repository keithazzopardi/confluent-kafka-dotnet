// Copyright 2016-2019 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Refer to LICENSE for more information.

using System.Threading.Tasks;
using System.Collections.Generic;
using Avro.Generic;
using Confluent.Kafka;
using System;
using System.Linq;
using Avro.Specific;
using System.Threading;
using System.IO;
using System.Net;
using Avro.IO;

namespace Confluent.SchemaRegistry.Serdes
{
    /// <summary>
    ///     (async) Avro deserializer. Use this deserializer with GenericRecord,
    ///     types generated using the avrogen.exe tool or one of the following 
    ///     primitive types: int, long, float, double, boolean, string, byte[].
    /// </summary>
    /// <remarks>
    ///     Serialization format:
    ///       byte 0:           Magic byte use to identify the protocol format.
    ///       bytes 1-4:        Unique global id of the Avro schema that was used for encoding (as registered in Confluent Schema Registry), big endian.
    ///       following bytes:  The serialized data.
    /// </remarks>
    public class GenericAvroDeserializer<T> : IAsyncDeserializer<T> where T : ISpecificRecord
    {
        private IAvroDeserializerImpl<T> deserializerImpl;

        private ISchemaRegistryClient schemaRegistryClient;

        /// <summary>
        ///     Initialize a new AvroDeserializer instance.
        /// </summary>
        /// <param name="schemaRegistryClient">
        ///     An implementation of ISchemaRegistryClient used for
        ///     communication with Confluent Schema Registry.
        /// </param>
        /// <param name="config">
        ///     Deserializer configuration properties (refer to 
        ///     <see cref="AvroDeserializerConfig" />).
        /// </param>
        public GenericAvroDeserializer(ISchemaRegistryClient schemaRegistryClient, IEnumerable<KeyValuePair<string, string>> config = null)
        {
            this.schemaRegistryClient = schemaRegistryClient;

            if (config == null) { return; }

            var nonAvroConfig = config.Where(item => !item.Key.StartsWith("avro."));
            if (nonAvroConfig.Count() > 0)
            {
                throw new ArgumentException($"AvroDeserializer: unknown configuration parameter {nonAvroConfig.First().Key}.");
            }

            var avroConfig = config.Where(item => item.Key.StartsWith("avro."));
            if (avroConfig.Count() != 0)
            {
                throw new ArgumentException($"AvroDeserializer: unknown configuration parameter {avroConfig.First().Key}");
            }
        }

        /// <summary>
        ///     Deserialize an object of type <typeparamref name="T"/>
        ///     from a byte array.
        /// </summary>
        /// <param name="data">
        ///     The raw byte data to deserialize.
        /// </param>
        /// <param name="isNull">
        ///     True if this is a null value.
        /// </param>
        /// <param name="context">
        ///     Context relevant to the deserialize operation.
        /// </param>
        /// <returns>
        ///     A <see cref="System.Threading.Tasks.Task" /> that completes
        ///     with the deserialized value.
        /// </returns>
        public async Task<T> DeserializeAsync(ReadOnlyMemory<byte> data, bool isNull, SerializationContext context)
        {
            try
            {
                if (deserializerImpl == null)
                {
                    deserializerImpl = new GenericSpecificDeserializerImpl<T>(schemaRegistryClient);
                }

                // TODO: change this interface such that it takes ReadOnlyMemory<byte>, not byte[].
                return await deserializerImpl.Deserialize(context.Topic, isNull ? null : data.ToArray());
            }
            catch (AggregateException e)
            {
                throw e.InnerException;
            }
        }
    }

    internal class GenericSpecificDeserializerImpl<T> : IAvroDeserializerImpl<T> where T : ISpecificRecord
    {
        /// <remarks>
        ///     A datum reader cache (one corresponding to each write schema that's been seen) 
        ///     is maintained so that they only need to be constructed once.
        /// </remarks>
        private readonly Dictionary<int, DatumReader<T>> datumReaderBySchemaId
            = new Dictionary<int, DatumReader<T>>();

        private SemaphoreSlim deserializeMutex = new SemaphoreSlim(1);

        private ISchemaRegistryClient schemaRegistryClient;

        public GenericSpecificDeserializerImpl(ISchemaRegistryClient schemaRegistryClient)
        {
            this.schemaRegistryClient = schemaRegistryClient;
        }

        public async Task<T> Deserialize(string topic, byte[] array)
        {
            try
            {
                // Note: topic is not necessary for deserialization (or knowing if it's a key 
                // or value) only the schema id is needed.

                using (var stream = new MemoryStream(array))
                using (var reader = new BinaryReader(stream))
                {
                    var magicByte = reader.ReadByte();
                    if (magicByte != Constants.MagicByte)
                    {
                        // may change in the future.
                        throw new InvalidDataException($"magic byte should be {Constants.MagicByte}, not {magicByte}");
                    }
                    var writerId = IPAddress.NetworkToHostOrder(reader.ReadInt32());

                    DatumReader<T> datumReader;
                    await deserializeMutex.WaitAsync().ConfigureAwait(continueOnCapturedContext: false);
                    try
                    {
                        datumReaderBySchemaId.TryGetValue(writerId, out datumReader);
                        if (datumReader == null)
                        {
                            if (datumReaderBySchemaId.Count > schemaRegistryClient.MaxCachedSchemas)
                            {
                                datumReaderBySchemaId.Clear();
                            }

                            var writerSchemaJson = await schemaRegistryClient.GetSchemaAsync(writerId).ConfigureAwait(continueOnCapturedContext: false);
                            var writerSchema = global::Avro.Schema.Parse(writerSchemaJson);

                            datumReader = new SpecificReader<T>(writerSchema, writerSchema);
                            datumReaderBySchemaId[writerId] = datumReader;
                        }
                    }
                    finally
                    {
                        deserializeMutex.Release();
                    }

                    return datumReader.Read(default(T), new BinaryDecoder(stream));
                }
            }
            catch (AggregateException e)
            {
                throw e.InnerException;
            }
        }

    }
}
