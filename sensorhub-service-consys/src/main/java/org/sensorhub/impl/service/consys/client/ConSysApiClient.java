/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.

Copyright (C) 2023 Sensia Software LLC. All Rights Reserved.

******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.consys.client;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Authenticator;
import java.net.HttpURLConnection;
import java.net.PasswordAuthentication;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandler;
import java.net.http.HttpResponse.BodyHandlers;
import java.net.http.HttpResponse.BodySubscriber;
import java.net.http.HttpResponse.BodySubscribers;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import net.opengis.swe.v20.BinaryEncoding;
import org.sensorhub.api.command.CommandStreamInfo;
import org.sensorhub.api.command.ICommandData;
import org.sensorhub.api.command.ICommandStreamInfo;
import org.sensorhub.api.data.DataStreamInfo;
import org.sensorhub.api.data.IDataStreamInfo;
import org.sensorhub.api.data.IObsData;
import org.sensorhub.api.procedure.IProcedureWithDesc;
import org.sensorhub.api.semantic.IDerivedProperty;
import org.sensorhub.api.system.ISystemWithDesc;
import org.sensorhub.impl.service.consys.ResourceParseException;
import org.sensorhub.impl.service.consys.obs.DataStreamBindingJson;
import org.sensorhub.impl.service.consys.obs.DataStreamSchemaBindingOmJson;
import org.sensorhub.impl.service.consys.procedure.ProcedureBindingGeoJson;
import org.sensorhub.impl.service.consys.procedure.ProcedureBindingSmlJson;
import org.sensorhub.impl.service.consys.property.PropertyBindingJson;
import org.sensorhub.impl.service.consys.obs.ObsBindingOmJson;
import org.sensorhub.impl.service.consys.obs.ObsBindingSweCommon;
import org.sensorhub.impl.service.consys.obs.ObsHandler;
import org.sensorhub.impl.service.consys.resource.RequestContext;
import org.sensorhub.impl.service.consys.resource.ResourceFormat;
import org.sensorhub.impl.service.consys.resource.ResourceLink;
import org.sensorhub.impl.service.consys.system.SystemBindingGeoJson;
import org.sensorhub.impl.service.consys.system.SystemBindingSmlJson;
import org.sensorhub.impl.service.consys.task.CommandBindingJson;
import org.sensorhub.impl.service.consys.task.CommandHandler;
import org.sensorhub.impl.service.consys.task.CommandStreamBindingJson;
import org.sensorhub.impl.service.consys.task.CommandStreamSchemaBindingJson;
import org.sensorhub.utils.Lambdas;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vast.util.Asserts;
import org.vast.util.BaseBuilder;
import com.google.common.base.Strings;
import com.google.common.net.HttpHeaders;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;


public class ConSysApiClient
{
    static final String PROPERTIES_COLLECTION = "properties";
    static final String PROCEDURES_COLLECTION = "procedures";
    static final String SYSTEMS_COLLECTION = "systems";
    static final String SUBSYSTEMS_COLLECTION = "subsystems";
    static final String DEPLOYMENTS_COLLECTION = "deployments";
    static final String DATASTREAMS_COLLECTION = "datastreams";
    static final String CONTROLS_COLLECTION = "controlstreams";
    static final String OBSERVATIONS_COLLECTION = "observations";
    static final String COMMANDS_COLLECTION = "commands";
    static final String SF_COLLECTION = "fois";
    static final String BINDING_ERROR = "Error initializing binding";

    static final Logger log = LoggerFactory.getLogger(ConSysApiClient.class);

    protected static boolean isHttpClientAvailable;

    static {
        // Check if HttpClient is available. Will not be available on Android.
        try {
            Class.forName("java.net.http.HttpClient");
            isHttpClientAvailable = true;
        } catch (ClassNotFoundException e) {
            isHttpClientAvailable = false;
        }
    }

    protected Authenticator authenticator;
    protected HttpClient http;
    protected URI endpoint;


    protected ConSysApiClient() {}
    
    
    /*------------*/
    /* Properties */
    /*------------*/
    
    public CompletableFuture<IDerivedProperty> getPropertyById(String id, ResourceFormat format)
    {
        return sendGetRequest(endpoint.resolve(PROPERTIES_COLLECTION + "/" + id), format, body -> {
            try
            {
                var ctx = new RequestContext(body);
                var binding = new PropertyBindingJson(ctx, null, null, true);
                return binding.deserialize();
            }
            catch (IOException e)
            {
                throw new CompletionException(e);
            }
        });
    }
    
    
    public CompletableFuture<IDerivedProperty> getPropertyByUri(String uri, ResourceFormat format)
    {
        try
        {
            return sendGetRequest(new URI(uri), format, body -> {
                try
                {
                    var ctx = new RequestContext(body);
                    var binding = new PropertyBindingJson(ctx, null, null, true);
                    return binding.deserialize();
                }
                catch (IOException e)
                {
                    throw new CompletionException(e);
                }
            });
        }
        catch (URISyntaxException e)
        {
            throw new IllegalArgumentException("Invalid property URI: " + uri);
        }
    }
    
    
    public CompletableFuture<String> addProperty(IDerivedProperty prop)
    {
        try
        {
            var buffer = new ByteArrayOutputStream();
            var ctx = new RequestContext(buffer);
            
            var binding = new PropertyBindingJson(ctx, null, null, false);
            binding.serialize(null, prop, false);
            
            return sendPostRequest(
                endpoint.resolve(PROPERTIES_COLLECTION),
                ResourceFormat.JSON,
                buffer.toByteArray());
        }
        catch (IOException e)
        {
            throw new IllegalStateException(BINDING_ERROR, e);
        }
    }
    
    
    public CompletableFuture<Set<String>> addProperties(IDerivedProperty... properties)
    {
        return addProperties(Arrays.asList(properties));
    }
    
    
    public CompletableFuture<Set<String>> addProperties(Collection<IDerivedProperty> properties)
    {
        try
        {
            var buffer = new ByteArrayOutputStream();
            var ctx = new RequestContext(buffer);
            
            var binding = new PropertyBindingJson(ctx, null, null, false) {
                @Override
                protected void startJsonCollection(JsonWriter writer) throws IOException
                {
                    writer.beginArray();
                }

                @Override
                protected void endJsonCollection(JsonWriter writer, Collection<ResourceLink> links) throws IOException
                {
                    writer.endArray();
                    writer.flush();
                }
            };
            
            binding.startCollection();
            for (var prop: properties)
                binding.serialize(null, prop, false);
            binding.endCollection(Collections.emptyList());
            
            return sendBatchPostRequest(
                endpoint.resolve(PROPERTIES_COLLECTION),
                ResourceFormat.JSON,
                buffer.toByteArray());
        }
        catch (IOException e)
        {
            throw new IllegalStateException(BINDING_ERROR, e);
        }
    }
    
    
    /*------------*/
    /* Procedures */
    /*------------*/
    
    public CompletableFuture<IProcedureWithDesc> getProcedureById(String id, ResourceFormat format)
    {
        return sendGetRequest(endpoint.resolve(PROCEDURES_COLLECTION + "/" + id), format, body -> {
            try
            {
                var ctx = new RequestContext(body);
                var binding = new ProcedureBindingGeoJson(ctx, null, null, true);
                return binding.deserialize();
            }
            catch (IOException e)
            {
                throw new CompletionException(e);
            }
        });
    }
    
    
    public CompletableFuture<IProcedureWithDesc> getProcedureByUid(String uid, ResourceFormat format)
    {
        return sendGetRequest(endpoint.resolve(PROCEDURES_COLLECTION + "?uid=" + uid), format, body -> {
            try
            {
                var ctx = new RequestContext(body);
                
                // use modified binding since the response contains a feature collection
                var binding = new ProcedureBindingGeoJson(ctx, null, null, true) {
                    @Override
                    public IProcedureWithDesc deserialize(JsonReader reader) throws IOException
                    {
                        skipToCollectionItems(reader);
                        return super.deserialize(reader);
                    }
                };
                
                return binding.deserialize();
            }
            catch (IOException e)
            {
                throw new CompletionException(e);
            }
        });
    }
    
    
    public CompletableFuture<String> addProcedure(IProcedureWithDesc system)
    {
        try
        {
            var buffer = new ByteArrayOutputStream();
            var ctx = new RequestContext(buffer);
            
            var binding = new ProcedureBindingSmlJson(ctx, null, false);
            binding.serialize(null, system, false);
            
            return sendPostRequest(
                endpoint.resolve(PROCEDURES_COLLECTION),
                ResourceFormat.SML_JSON,
                buffer.toByteArray());
        }
        catch (IOException e)
        {
            throw new IllegalStateException(BINDING_ERROR, e);
        }
    }
    
    
    public CompletableFuture<Set<String>> addProcedures(IProcedureWithDesc... systems)
    {
        return addProcedures(Arrays.asList(systems));
    }
    
    
    public CompletableFuture<Set<String>> addProcedures(Collection<IProcedureWithDesc> systems)
    {
        try
        {
            var buffer = new ByteArrayOutputStream();
            var ctx = new RequestContext(buffer);
            
            var binding = new ProcedureBindingSmlJson(ctx, null, false) {
                @Override
                protected void startJsonCollection(JsonWriter writer) throws IOException
                {
                    writer.beginArray();
                }

                @Override
                protected void endJsonCollection(JsonWriter writer, Collection<ResourceLink> links) throws IOException
                {
                    writer.endArray();
                    writer.flush();
                }
            };
            
            binding.startCollection();
            for (var sys: systems)
                binding.serialize(null, sys, false);
            binding.endCollection(Collections.emptyList());
            
            return sendBatchPostRequest(
                endpoint.resolve(PROCEDURES_COLLECTION),
                ResourceFormat.SML_JSON,
                buffer.toByteArray());
        }
        catch (IOException e)
        {
            throw new IllegalStateException(BINDING_ERROR, e);
        }
    }
    
    
    /*---------*/
    /* Systems */
    /*---------*/

    /**
     * List all top level System resources available from this server endpoint (i.e., subsystems are omitted).
     *
     * @param format The format of the response
     * @return A list of system descriptions
     */
    public CompletableFuture<List<ISystemWithDesc>> getSystems(ResourceFormat format)
    {
        return getSystems(format, "");
    }

    /**
     * List or search all System resources available from this server endpoint.
     * By default, only top level systems are included (i.e., subsystems are omitted)
     * unless the <code>parent</code> query parameter is set.
     *
     * @param format The format of the response
     * @param query  Optional query string to filter the results
     * @return A list of system descriptions
     */
    public CompletableFuture<List<ISystemWithDesc>> getSystems(ResourceFormat format, String query)
    {
        query = query == null ? "" : query;

        return sendGetRequest(endpoint.resolve(SYSTEMS_COLLECTION + query), format, body ->
                getCollectionItems(body, itemBody -> {
                    try
                    {
                        var ctx = new RequestContext(itemBody);
                        var binding = new SystemBindingGeoJson(ctx, null, null, true);
                        return binding.deserialize();
                    }
                    catch (IOException e)
                    {
                        throw new CompletionException(e);
                    }
                })
        );
    }

    /**
     * Return the latest description of the system valid before or at the current time.
     *
     * @param id     Local identifier of the system
     * @param format The format of the response
     * @return The system description
     */
    public CompletableFuture<ISystemWithDesc> getSystemById(String id, ResourceFormat format)
    {
        return getSystemById(id, format, "");
    }

    /**
     * Return the latest description of the system valid before or at the current time, by default.
     * If the server supports system history, descriptions of the system valid at past (or future)
     * time can be accessed using the <code>datetime</code> parameter or through the <code>history</code> subcollection.
     *
     * @param id     Local identifier of the system
     * @param format The format of the response
     * @param query  Optional query string to filter the results
     * @return The system description
     */
    public CompletableFuture<ISystemWithDesc> getSystemById(String id, ResourceFormat format, String query)
    {
        query = query == null ? "" : query;

        return sendGetRequest(endpoint.resolve(SYSTEMS_COLLECTION + "/" + id + query), format, body -> {
            try
            {
                var ctx = new RequestContext(body);
                var binding = new SystemBindingGeoJson(ctx, null, null, true);
                return binding.deserialize();
            }
            catch (IOException e)
            {
                throw new CompletionException(e);
            }
        });
    }

    /**
     * Return the latest description of the system valid before or at the current time.
     *
     * @param uid    The UID of the system
     * @param format The format of the response
     * @return The system description
     */
    public CompletableFuture<ISystemWithDesc> getSystemByUid(String uid, ResourceFormat format)
    {
        return sendGetRequest(endpoint.resolve(SYSTEMS_COLLECTION + "?uid=" + uid), format, body -> {
            try
            {
                var ctx = new RequestContext(body);
                
                // use modified binding since the response contains a feature collection
                var binding = new SystemBindingGeoJson(ctx, null, null, true) {
                    @Override
                    public ISystemWithDesc deserialize(JsonReader reader) throws IOException
                    {
                        skipToCollectionItems(reader);
                        return super.deserialize(reader);
                    }
                };
                
                return binding.deserialize();
            }
            catch (IOException e)
            {
                throw new CompletionException(e);
            }
        });
    }

    /**
     * List all System resources that are subsystems (i.e., components) of a specific parent system.
     *
     * @param systemId The local identifier of the parent system
     * @param format   The format of the response
     * @return A list of system descriptions
     */
    public CompletableFuture<List<ISystemWithDesc>> getSubsystems(String systemId, ResourceFormat format)
    {
        return getSubsystems(systemId, format, "");
    }

    /**
     * List or search all System resources that are subsystems (i.e., components) of a specific parent system.
     *
     * @param systemId The local identifier of the parent system
     * @param format   The format of the response
     * @param query    Optional query string to filter the results
     * @return A list of system descriptions
     */
    public CompletableFuture<List<ISystemWithDesc>> getSubsystems(String systemId, ResourceFormat format, String query)
    {
        query = query == null ? "" : query;

        return sendGetRequest(endpoint.resolve(SYSTEMS_COLLECTION + "/" + systemId + "/" + SUBSYSTEMS_COLLECTION + query), format, body ->
                getCollectionItems(body, itemBody -> {
                    try
                    {
                        var ctx = new RequestContext(itemBody);
                        var binding = new SystemBindingGeoJson(ctx, null, null, true);
                        return binding.deserialize();
                    }
                    catch (IOException e)
                    {
                        throw new CompletionException(e);
                    }
                })
        );
    }

    /**
     * Add a new top-level <code>System</code> resource (i.e., the system will have no parent).
     *
     * @param system The description of the system to be added
     * @return The local identifier of the new system
     */
    public CompletableFuture<String> addSystem(ISystemWithDesc system)
    {
        try
        {
            var buffer = new ByteArrayOutputStream();
            var ctx = new RequestContext(buffer);

            var binding = new SystemBindingSmlJson(ctx, null, false);
            binding.serialize(null, system, false);

            return sendPostRequest(
                endpoint.resolve(SYSTEMS_COLLECTION),
                ResourceFormat.SML_JSON,
                buffer.toByteArray());
        }
        catch (IOException e)
        {
            throw new IllegalStateException(BINDING_ERROR, e);
        }
    }

    /**
     * This will completely replace the existing description of the system with the provided content.
     * If system history is supported and the <code>validTime</code> property starts after the time of the previous description,
     * the provided description becomes the current one,
     * and all previous descriptions are made available via the <code>history</code> subcollection.
     *
     * @param systemId Local identifier of the system to be updated
     * @param system   The new description of the system
     * @return The HTTP status code of the response
     */
    public CompletableFuture<Integer> updateSystem(String systemId, ISystemWithDesc system)
    {
        try
        {
            var buffer = new ByteArrayOutputStream();
            var ctx = new RequestContext(buffer);

            var binding = new SystemBindingSmlJson(ctx, null, false);
            binding.serialize(null, system, false);

            return sendPutRequest(
                    endpoint.resolve(SYSTEMS_COLLECTION + "/" + systemId),
                    ResourceFormat.SML_JSON,
                    buffer.toByteArray());
        }
        catch (IOException e)
        {
            throw new IllegalStateException(BINDING_ERROR, e);
        }
    }

    /**
     * Add a new subsystem to the system with the given ID.
     *
     * @param systemId Local identifier of the parent system
     * @param system   The subsystem to be added
     * @return The local identifier of the new subsystem
     */
    public CompletableFuture<String> addSubSystem(String systemId, ISystemWithDesc system)
    {
        try
        {
            var buffer = new ByteArrayOutputStream();
            var ctx = new RequestContext(buffer);

            var binding = new SystemBindingSmlJson(ctx, null, false);
            binding.serialize(null, system, false);

            return sendPostRequest(
                    endpoint.resolve(SYSTEMS_COLLECTION + "/" + systemId + "/" + SUBSYSTEMS_COLLECTION),
                    ResourceFormat.SML_JSON,
                    buffer.toByteArray());
        }
        catch (IOException e)
        {
            throw new IllegalStateException(BINDING_ERROR, e);
        }
    }

    public CompletableFuture<Set<String>> addSystems(ISystemWithDesc... systems)
    {
        return addSystems(Arrays.asList(systems));
    }

    public CompletableFuture<Set<String>> addSystems(Collection<ISystemWithDesc> systems)
    {
        try
        {
            var buffer = new ByteArrayOutputStream();
            var ctx = new RequestContext(buffer);

            var binding = new SystemBindingSmlJson(ctx, null, false) {
                @Override
                protected void startJsonCollection(JsonWriter writer) throws IOException
                {
                    writer.beginArray();
                }

                @Override
                protected void endJsonCollection(JsonWriter writer, Collection<ResourceLink> links) throws IOException
                {
                    writer.endArray();
                    writer.flush();
                }
            };

            binding.startCollection();
            for (var sys: systems)
                binding.serialize(null, sys, false);
            binding.endCollection(Collections.emptyList());

            return sendBatchPostRequest(
                endpoint.resolve(SYSTEMS_COLLECTION),
                ResourceFormat.SML_JSON,
                buffer.toByteArray());
        }
        catch (IOException e)
        {
            throw new IllegalStateException(BINDING_ERROR, e);
        }
    }

    /**
     * Delete the system and remove it from all collections it is associated to.
     * If the <code>cascade</code> parameter is used,
     * all associated sub-resources hosted by the same server
     * (sampling features, datastreams, command streams, observations, and commands) are also deleted.
     * If system history is supported, all historical descriptions are deleted as well.
     *
     * @param systemId Local identifier of a System
     * @return The HTTP status code of the response
     */
    public CompletableFuture<Integer> deleteSystem(String systemId)
    {
        return sendDeleteRequest(endpoint.resolve(SYSTEMS_COLLECTION + "/" + systemId));
    }

    /**
     * Delete the system and remove it from all collections it is associated to.
     * If the <code>cascade</code> parameter is used,
     * all associated sub-resources hosted by the same server
     * (sampling features, datastreams, command streams, observations, and commands) are also deleted.
     * If system history is supported, all historical descriptions are deleted as well.
     *
     * @param systemId Local identifier of a System
     * @param cascade  If set to true, dependent resources are also deleted
     * @return The HTTP status code of the response
     */
    public CompletableFuture<Integer> deleteSystem(String systemId, boolean cascade)
    {
        return sendDeleteRequest(endpoint.resolve(SYSTEMS_COLLECTION + "/" + systemId + "?cascade=" + cascade));
    }


    /*-------------*/
    /* Datastreams */
    /*-------------*/

    /**
     * List all datastreams available from this server endpoint.
     *
     * @param format      The format of the response
     * @param fetchSchema If true, the datastream schema is also fetched
     * @return A list of datastream descriptions
     */
    public CompletableFuture<List<IDataStreamInfo>> getDataStreams(ResourceFormat format, boolean fetchSchema)
    {
        return getDataStreams(format, fetchSchema, "");
    }

    /**
     * List or search all datastreams available from this server endpoint.
     *
     * @param format      The format of the response
     * @param fetchSchema If true, the datastream schema is also fetched
     * @param query       Optional query string to filter the results
     * @return A list of datastream descriptions
     */
    public CompletableFuture<List<IDataStreamInfo>> getDataStreams(ResourceFormat format, boolean fetchSchema, String query)
    {
        query = query == null ? "" : query;

        var cf1 = sendGetRequest(endpoint.resolve(DATASTREAMS_COLLECTION + query), format, body ->
                getCollectionItems(body, itemBody -> {
                    try
                    {
                        var ctx = new RequestContext(itemBody);
                        var binding = new DataStreamBindingJson(ctx, null, null, true, Collections.emptyMap());
                        return binding.deserialize();
                    }
                    catch (IOException e)
                    {
                        throw new CompletionException(e);
                    }
                })
        );

        if (fetchSchema)
        {
            return cf1.thenApply(dsList -> {
                List<IDataStreamInfo> dsListNew = new ArrayList<>();
                for (var dsInfo : dsList)
                {
                    var schemaInfo = getDataStreamSchema(dsInfo.getID(), ResourceFormat.JSON, ResourceFormat.JSON).join();
                    schemaInfo.getRecordStructure().setName(dsInfo.getOutputName());
                    dsInfo = DataStreamInfo.Builder.from(dsInfo)
                            .withRecordDescription(schemaInfo.getRecordStructure())
                            .build();
                    dsListNew.add(dsInfo);
                }
                return dsListNew;
            });
        } else
            return cf1;
    }

    /**
     * List all datastreams available from the parent system.
     *
     * @param systemId    The local identifier of the parent system
     * @param format      The format of the response
     * @param fetchSchema If true, the datastream schema is also fetched
     * @return A list of datastream descriptions
     */
    public CompletableFuture<List<IDataStreamInfo>> getDataStreamsOfSystem(String systemId, ResourceFormat format, boolean fetchSchema)
    {
        return getDataStreamsOfSystem(systemId, format, fetchSchema, "");
    }

    /**
     * List or search all datastreams available from the parent system.
     *
     * @param systemId    The local identifier of the parent system
     * @param format      The format of the response
     * @param fetchSchema If true, the datastream schema is also fetched
     * @param query       Optional query string to filter the results
     * @return A list of datastream descriptions
     */
    public CompletableFuture<List<IDataStreamInfo>> getDataStreamsOfSystem(String systemId, ResourceFormat format, boolean fetchSchema, String query)
    {
        query = query == null ? "" : query;

        var cf1 = sendGetRequest(endpoint.resolve(SYSTEMS_COLLECTION + "/" + systemId + "/" + DATASTREAMS_COLLECTION + query), format, body ->
                getCollectionItems(body, itemBody -> {
                    try
                    {
                        var ctx = new RequestContext(itemBody);
                        var binding = new DataStreamBindingJson(ctx, null, null, true, Collections.emptyMap());
                        return binding.deserialize();
                    }
                    catch (IOException e)
                    {
                        throw new CompletionException(e);
                    }
                })
        );

        if (fetchSchema)
        {
            return cf1.thenApply(dsList -> {
                List<IDataStreamInfo> dsListNew = new ArrayList<>();
                for (var dsInfo : dsList)
                {
                    var schemaInfo = getDataStreamSchema(dsInfo.getID(), ResourceFormat.JSON, ResourceFormat.JSON).join();
                    schemaInfo.getRecordStructure().setName(dsInfo.getOutputName());
                    dsInfo = DataStreamInfo.Builder.from(dsInfo)
                            .withRecordDescription(schemaInfo.getRecordStructure())
                            .build();
                    dsListNew.add(dsInfo);
                }
                return dsListNew;
            });
        } else
            return cf1;
    }

    /**
     * Get the datastream description by its local identifier.
     *
     * @param id          The local identifier of the datastream
     * @param format      The format of the response
     * @param fetchSchema If true, the datastream schema is also fetched
     * @return The datastream description
     */
    public CompletableFuture<IDataStreamInfo> getDataStreamById(String id, ResourceFormat format, boolean fetchSchema)
    {
        var cf1 = sendGetRequest(endpoint.resolve(DATASTREAMS_COLLECTION + "/" + id), format, body -> {
            try
            {
                var ctx = new RequestContext(body);
                var binding = new DataStreamBindingJson(ctx, null, null, true, Collections.emptyMap());
                return binding.deserialize();
            }
            catch (IOException e)
            {
                throw new CompletionException(e);
            }
        });
        
        if (fetchSchema)
        {
            return cf1.thenCombine(getDataStreamSchema(id, ResourceFormat.JSON, ResourceFormat.JSON), (dsInfo, schemaInfo) -> {
                
                schemaInfo.getRecordStructure().setName(dsInfo.getOutputName());
                
                dsInfo = DataStreamInfo.Builder.from(dsInfo)
                    .withRecordDescription(schemaInfo.getRecordStructure())
                    .build();
                
                return dsInfo;
            });
        }
        else
            return cf1;
        
    }

    /**
     * Get the observation schema for a given format.
     * The type of observation schema returned depends on the observation encoding format
     * selected using the <code>obsFormat</code> parameter.
     *
     * @param id        The local identifier of the datastream
     * @param obsFormat The encoding format of the observations
     * @param format    The format of the response
     * @return The datastream schema
     */
    public CompletableFuture<IDataStreamInfo> getDataStreamSchema(String id, ResourceFormat obsFormat, ResourceFormat format)
    {
        return sendGetRequest(endpoint.resolve(DATASTREAMS_COLLECTION + "/" + id + "/schema?obsFormat="+obsFormat), format, body -> {
            try
            {
                var ctx = new RequestContext(body);
                var binding = new DataStreamSchemaBindingOmJson(ctx, null, true);
                return binding.deserialize();
            }
            catch (IOException e)
            {
                throw new CompletionException(e);
            }
        });
    }

    /**
     * Add a new datastream to an existing system.
     *
     * @param systemId   The local identifier of the parent system
     * @param datastream The datastream to be added
     * @return The local identifier of the new datastream
     */
    public CompletableFuture<String> addDataStream(String systemId, IDataStreamInfo datastream)
    {
        try
        {
            var buffer = new ByteArrayOutputStream();
            var ctx = new RequestContext(buffer);
            
            var binding = new DataStreamBindingJson(ctx, null, null, false, Collections.emptyMap());
            binding.serialize(null, datastream, false);

            return sendPostRequest(
                endpoint.resolve(SYSTEMS_COLLECTION + "/" + systemId + "/" + DATASTREAMS_COLLECTION),
                ResourceFormat.JSON,
                buffer.toByteArray());
        }
        catch (IOException e)
        {
            throw new IllegalStateException(BINDING_ERROR, e);
        }
    }

    public CompletableFuture<Set<String>> addDataStreams(String systemId, IDataStreamInfo... datastreams)
    {
        return addDataStreams(systemId, Arrays.asList(datastreams));
    }

    public CompletableFuture<Set<String>> addDataStreams(String systemId, Collection<IDataStreamInfo> datastreams)
    {
        try
        {
            var buffer = new ByteArrayOutputStream();
            var ctx = new RequestContext(buffer);
            
            var binding = new DataStreamBindingJson(ctx, null, null, false, Collections.emptyMap()) {
                @Override
                protected void startJsonCollection(JsonWriter writer) throws IOException
                {
                    writer.beginArray();
                }

                @Override
                protected void endJsonCollection(JsonWriter writer, Collection<ResourceLink> links) throws IOException
                {
                    writer.endArray();
                    writer.flush();
                }
            };

            binding.startCollection();
            for (var ds: datastreams)
                binding.serialize(null, ds, false);
            binding.endCollection(Collections.emptyList());

            return sendBatchPostRequest(
                endpoint.resolve(SYSTEMS_COLLECTION + "/" + systemId + "/" + DATASTREAMS_COLLECTION),
                ResourceFormat.JSON,
                buffer.toByteArray());
        }
        catch (IOException e)
        {
            throw new IllegalStateException(BINDING_ERROR, e);
        }
    }

    /**
     * Update the datastream description.
     *
     * @param dataStreamId The local identifier of the datastream to be updated
     * @param dataStream   The new datastream description
     * @return The HTTP status code of the response
     */
    public CompletableFuture<Integer> updateDataStream(String dataStreamId, IDataStreamInfo dataStream)
    {
        try
        {
            var buffer = new ByteArrayOutputStream();
            var ctx = new RequestContext(buffer);

            var binding = new DataStreamBindingJson(ctx, null, null, false, Collections.emptyMap());
            binding.serialize(null, dataStream, false);

            return sendPutRequest(
                    endpoint.resolve(DATASTREAMS_COLLECTION + "/" + dataStreamId),
                    ResourceFormat.JSON,
                    buffer.toByteArray());
        }
        catch (IOException e)
        {
            throw new IllegalStateException(BINDING_ERROR, e);
        }
    }

    /**
     * This will delete the datastream and remove it from all collections it is associated to.
     * If the <code>cascade</code> parameter is used, all associated observations are also deleted.
     *
     * @param dataStreamId The local identifier of the datastream to be deleted
     * @return The HTTP status code of the response
     */
    public CompletableFuture<Integer> deleteDataStream(String dataStreamId)
    {
        return sendDeleteRequest(endpoint.resolve(DATASTREAMS_COLLECTION + "/" + dataStreamId));
    }

    /**
     * This will delete the datastream and remove it from all collections it is associated to.
     * If the <code>cascade</code> parameter is used, all associated observations are also deleted.
     *
     * @param dataStreamId The local identifier of the datastream to be deleted
     * @return The HTTP status code of the response
     */
    public CompletableFuture<Integer> deleteDataStream(String dataStreamId, boolean cascade)
    {
        return sendDeleteRequest(endpoint.resolve(DATASTREAMS_COLLECTION + "/" + dataStreamId + "?cascade=" + cascade));
    }


    /*-----------------*/
    /* Control Streams */
    /*-----------------*/


    /**
     * List all control streams available from this server endpoint.
     *
     * @param format      The format of the response
     * @param fetchSchema If true, the control stream schema is also fetched
     * @return A list of control stream descriptions
     */
    public CompletableFuture<List<ICommandStreamInfo>> getControlStreams(ResourceFormat format, boolean fetchSchema)
    {
        return getControlStreams(format, fetchSchema, "");
    }

    /**
     * List or search all control streams available from this server endpoint.
     *
     * @param format      The format of the response
     * @param fetchSchema If true, the control stream schema is also fetched
     * @param query       Optional query string to filter the results
     * @return A list of control stream descriptions
     */
    public CompletableFuture<List<ICommandStreamInfo>> getControlStreams(ResourceFormat format, boolean fetchSchema, String query)
    {
        query = query == null ? "" : query;

        var cf1 = sendGetRequest(endpoint.resolve(CONTROLS_COLLECTION + query), format, body ->
                getCollectionItems(body, itemBody -> {
                    try
                    {
                        var ctx = new RequestContext(itemBody);
                        var binding = new CommandStreamBindingJson(ctx, null, null, true);
                        return binding.deserialize();
                    }
                    catch (IOException e)
                    {
                        throw new CompletionException(e);
                    }
                })
        );

        if (fetchSchema)
        {
            return cf1.thenApply(csList -> {
                List<ICommandStreamInfo> csListNew = new ArrayList<>();
                for (var csInfo : csList)
                {
                    var schemaInfo = getControlStreamSchema(csInfo.getID(), ResourceFormat.JSON, ResourceFormat.JSON).join();
                    schemaInfo.getRecordStructure().setName(csInfo.getControlInputName());
                    csInfo = CommandStreamInfo.Builder.from(csInfo)
                            .withRecordDescription(schemaInfo.getRecordStructure())
                            .build();
                    csListNew.add(csInfo);
                }
                return csListNew;
            });
        } else
            return cf1;
    }

    /**
     * List all control streams available from the parent system.
     *
     * @param systemId    The local identifier of the parent system
     * @param format      The format of the response
     * @param fetchSchema If true, the control stream schema is also fetched
     * @return A list of control stream descriptions
     */
    public CompletableFuture<List<ICommandStreamInfo>> getControlStreamsOfSystem(String systemId, ResourceFormat format, boolean fetchSchema)
    {
        return getControlStreamsOfSystem(systemId, format, fetchSchema, "");
    }

    /**
     * List or search all control streams available from the parent system.
     *
     * @param format      The format of the response
     * @param fetchSchema If true, the control stream schema is also fetched
     * @param query       Optional query string to filter the results
     * @return A list of control stream descriptions
     */
    public CompletableFuture<List<ICommandStreamInfo>> getControlStreamsOfSystem(String systemId, ResourceFormat format, boolean fetchSchema, String query)
    {
        query = query == null ? "" : query;

        var cf1 = sendGetRequest(endpoint.resolve(SYSTEMS_COLLECTION + "/" + systemId + "/" + CONTROLS_COLLECTION + query), format, body ->
                getCollectionItems(body, itemBody -> {
                    try
                    {
                        var ctx = new RequestContext(itemBody);
                        var binding = new CommandStreamBindingJson(ctx, null, null, true);
                        return binding.deserialize();
                    }
                    catch (IOException e)
                    {
                        throw new CompletionException(e);
                    }
                })
        );

        if (fetchSchema)
        {
            return cf1.thenApply(csList -> {
                List<ICommandStreamInfo> csListNew = new ArrayList<>();
                for (var csInfo : csList)
                {
                    var schemaInfo = getControlStreamSchema(csInfo.getID(), ResourceFormat.JSON, ResourceFormat.JSON).join();
                    schemaInfo.getRecordStructure().setName(csInfo.getControlInputName());
                    csInfo = CommandStreamInfo.Builder.from(csInfo)
                            .withRecordDescription(schemaInfo.getRecordStructure())
                            .build();
                    csListNew.add(csInfo);
                }
                return csListNew;
            });
        } else
            return cf1;
    }

    /**
     * Get the control stream description by its local identifier.
     *
     * @param id          The local identifier of the control stream
     * @param format      The format of the response
     * @param fetchSchema If true, the control stream schema is also fetched
     * @return The control stream description
     */
    public CompletableFuture<ICommandStreamInfo> getControlStreamById(String id, ResourceFormat format, boolean fetchSchema)
    {
        var cf1 = sendGetRequest(endpoint.resolve(CONTROLS_COLLECTION + "/" + id), format, body -> {
            try
            {
                var ctx = new RequestContext(body);
                var binding = new CommandStreamBindingJson(ctx, null, null, true);
                return binding.deserialize();
            }
            catch (IOException e)
            {
                throw new CompletionException(e);
            }
        });

        if (fetchSchema)
        {
            return cf1.thenCombine(getControlStreamSchema(id, ResourceFormat.JSON, ResourceFormat.JSON), (csInfo, schemaInfo) -> {

                schemaInfo.getRecordStructure().setName(csInfo.getControlInputName());

                csInfo = CommandStreamInfo.Builder.from(csInfo)
                        .withRecordDescription(schemaInfo.getRecordStructure())
                        .build();

                return csInfo;
            });
        } else
            return cf1;
    }

    /**
     * Get the control stream schema for a given format.
     * The type of control stream schema returned depends on the encoding format
     * selected using the <code>commandFormat</code> parameter.
     *
     * @param id            The local identifier of the control stream
     * @param commandFormat The encoding format of the observations
     * @param format        The format of the response
     * @return The control stream schema
     */
    public CompletableFuture<ICommandStreamInfo> getControlStreamSchema(String id, ResourceFormat commandFormat, ResourceFormat format)
    {
        return sendGetRequest(endpoint.resolve(CONTROLS_COLLECTION + "/" + id + "/schema?cmdFormat=" + commandFormat), format, body -> {
            try
            {
                var ctx = new RequestContext(body);
                var binding = new CommandStreamSchemaBindingJson(ctx, null, true);
                return binding.deserialize();
            }
            catch (IOException e)
            {
                throw new CompletionException(e);
            }
        });
    }

    /**
     * Add a new control stream to an existing system.
     *
     * @param systemId      The local identifier of the parent system
     * @param controlStream The control stream to be added
     * @return The local identifier of the new control stream
     */
    public CompletableFuture<String> addControlStream(String systemId, ICommandStreamInfo controlStream)
    {
        try
        {
            var buffer = new ByteArrayOutputStream();
            var ctx = new RequestContext(buffer);
            
            var binding = new CommandStreamBindingJson(ctx, null, null, false);
            binding.serializeCreate(controlStream);

            return sendPostRequest(
                endpoint.resolve(SYSTEMS_COLLECTION + "/" + systemId + "/" + CONTROLS_COLLECTION),
                ResourceFormat.JSON,
                buffer.toByteArray());
        }
        catch (IOException e)
        {
            throw new IllegalStateException(BINDING_ERROR, e);
        }
    }


    public CompletableFuture<Set<String>> addControlStreams(String systemId, ICommandStreamInfo... controlStreams)
    {
        return addControlStreams(systemId, Arrays.asList(controlStreams));
    }


    public CompletableFuture<Set<String>> addControlStreams(String systemId, Collection<ICommandStreamInfo> controlStreams)
    {
        try
        {
            var buffer = new ByteArrayOutputStream();
            var ctx = new RequestContext(buffer);
            
            var binding = new CommandStreamBindingJson(ctx, null, null, false) {
                @Override
                protected void startJsonCollection(JsonWriter writer) throws IOException
                {
                    writer.beginArray();
                }

                @Override
                protected void endJsonCollection(JsonWriter writer, Collection<ResourceLink> links) throws IOException
                {
                    writer.endArray();
                    writer.flush();
                }
            };

            binding.startCollection();
            for (var cs : controlStreams)
                binding.serializeCreate(cs);
            binding.endCollection(Collections.emptyList());

            return sendBatchPostRequest(
                endpoint.resolve(SYSTEMS_COLLECTION + "/" + systemId + "/" + CONTROLS_COLLECTION),
                ResourceFormat.JSON,
                buffer.toByteArray());
        }
        catch (IOException e)
        {
            throw new IllegalStateException(BINDING_ERROR, e);
        }
    }

    /**
     * Update the control stream description.
     *
     * @param controlStreamId The local identifier of the control stream to be updated
     * @param controlStream   The new control stream description
     * @return The HTTP status code of the response
     */
    public CompletableFuture<Integer> updateControlStream(String controlStreamId, ICommandStreamInfo controlStream)
    {
        try
        {
            var buffer = new ByteArrayOutputStream();
            var ctx = new RequestContext(buffer);

            var binding = new CommandStreamBindingJson(ctx, null, null, false);
            binding.serialize(null, controlStream, false);

            return sendPutRequest(
                    endpoint.resolve(CONTROLS_COLLECTION + "/" + controlStreamId),
                    ResourceFormat.JSON,
                    buffer.toByteArray());
        }
        catch (IOException e)
        {
            throw new IllegalStateException(BINDING_ERROR, e);
        }
    }

    /**
     * This will delete the control stream and remove it from all collections it is associated to.
     * If the <code>cascade</code> parameter is used, all associated commands are also deleted.
     *
     * @param controlStreamId The local identifier of the control stream to be deleted
     * @return The HTTP status code of the response
     */
    public CompletableFuture<Integer> deleteControlStream(String controlStreamId)
    {
        return sendDeleteRequest(endpoint.resolve(CONTROLS_COLLECTION + "/" + controlStreamId));
    }

    /**
     * This will delete the control stream and remove it from all collections it is associated to.
     * If the <code>cascade</code> parameter is used, all associated commands are also deleted.
     *
     * @param controlStreamId The local identifier of the control stream to be deleted
     * @return The HTTP status code of the response
     */
    public CompletableFuture<Integer> deleteControlStream(String controlStreamId, boolean cascade)
    {
        return sendDeleteRequest(endpoint.resolve(CONTROLS_COLLECTION + "/" + controlStreamId + "?cascade=" + cascade));
    }


    /*--------------*/
    /* Observations */
    /*--------------*/

    /**
     * List all observations available from a datastream.
     *
     * @param dataStream The datastream description
     * @param format     The format of the response
     * @return A list of observations
     */
    public CompletableFuture<List<IObsData>> getObservationsOfDataStream(IDataStreamInfo dataStream, ResourceFormat format)
    {
        return getObservationsOfDataStream(dataStream, format, null);
    }

    /**
     * List or search all observations available from a datastream.
     *
     * @param dataStream The datastream description
     * @param format     The format of the response
     * @param query      Optional query string to filter the results
     * @return A list of observations
     */
    public CompletableFuture<List<IObsData>> getObservationsOfDataStream(IDataStreamInfo dataStream, ResourceFormat format, String query)
    {
        query = query == null ? "" : query;

        return sendGetRequest(endpoint.resolve(DATASTREAMS_COLLECTION + "/" + dataStream.getID() + "/" + OBSERVATIONS_COLLECTION + query), format, body ->
                getCollectionItems(body, itemBody -> {
                    try
                    {
                        ObsHandler.ObsHandlerContextData contextData = new ObsHandler.ObsHandlerContextData();
                        contextData.dsInfo = dataStream;

                        var ctx = new RequestContext(itemBody);
                        ctx.setData(contextData);

                        if (dataStream.getRecordEncoding() instanceof BinaryEncoding)
                        {
                            ctx.setFormat(ResourceFormat.SWE_BINARY);
                            var binding = new ObsBindingSweCommon(ctx, null, true, null);
                            return binding.deserialize();
                        } else
                        {
                            ctx.setFormat(ResourceFormat.OM_JSON);
                            var binding = new ObsBindingOmJson(ctx, null, true, null);
                            return binding.deserialize();
                        }
                    }
                    catch (IOException e)
                    {
                        throw new CompletionException(e);
                    }
                })
        );
    }

    /**
     * Get the observation by its local identifier.
     *
     * @param observationId The local identifier of the observation
     * @param format        The format of the response
     * @return The observation
     */
    public CompletableFuture<IObsData> getObservationById(String observationId, ResourceFormat format, IDataStreamInfo dataStream)
    {
        return sendGetRequest(endpoint.resolve(OBSERVATIONS_COLLECTION + "/" + observationId), format, body -> {
            try
            {
                ObsHandler.ObsHandlerContextData contextData = new ObsHandler.ObsHandlerContextData();
                contextData.dsInfo = dataStream;

                var ctx = new RequestContext(body);
                ctx.setData(contextData);

                if (dataStream != null && dataStream.getRecordEncoding() instanceof BinaryEncoding)
                {
                    ctx.setFormat(ResourceFormat.SWE_BINARY);
                    var binding = new ObsBindingSweCommon(ctx, null, true, null);
                    return binding.deserialize();
                } else
                {
                    ctx.setFormat(ResourceFormat.OM_JSON);
                    var binding = new ObsBindingOmJson(ctx, null, true, null);
                    return binding.deserialize();
                }
            }
            catch (IOException e)
            {
                throw new CompletionException(e);
            }
        });
    }

    /**
     * Add a new observation to an existing datastream.
     *
     * @param dataStreamId The local identifier of the datastream
     * @param dataStream   The datastream description
     * @param obs          The observation to be added
     * @return The local identifier of the new observation
     */
    public CompletableFuture<String> pushObservation(String dataStreamId, IDataStreamInfo dataStream, IObsData obs)
    {
        // TODO: Be able to push different kinds of observations such as video
        try
        {
            ObsHandler.ObsHandlerContextData contextData = new ObsHandler.ObsHandlerContextData();
            contextData.dsInfo = dataStream;

            var buffer = new ByteArrayOutputStream();
            var ctx = new RequestContext(buffer);
            ctx.setData(contextData);

            if(dataStream != null && dataStream.getRecordEncoding() instanceof BinaryEncoding) {
                ctx.setFormat(ResourceFormat.SWE_BINARY);
                var binding = new ObsBindingSweCommon(ctx, null, false, null);
                binding.serialize(null, obs, false);
            } else {
                ctx.setFormat(ResourceFormat.OM_JSON);
                var binding = new ObsBindingOmJson(ctx, null, false, null);
                binding.serialize(null, obs, false);
            }

            return sendPostRequest(
                    endpoint.resolve(DATASTREAMS_COLLECTION + "/" + dataStreamId + "/" + OBSERVATIONS_COLLECTION),
                    ctx.getFormat(),
                    buffer.toByteArray());
        }
        catch (IOException e)
        {
            throw new IllegalStateException(BINDING_ERROR, e);
        }
    }


    /*----------*/
    /* Commands */
    /*----------*/

    /**
     * List all commands received in a specific control stream.
     *
     * @param commandStreamInfo The command stream description
     * @param format            The format of the response
     * @return A list of commands
     */
    public CompletableFuture<List<ICommandData>> getCommandsOfControlStream(ICommandStreamInfo commandStreamInfo, ResourceFormat format)
    {
        return getCommandsOfControlStream(commandStreamInfo, format, "");
    }

    /**
     * List or search all commands received in a specific control stream.
     *
     * @param commandStreamInfo The command stream description
     * @param format            The format of the response
     * @param query             Optional query string to filter the results
     * @return A list of commands
     */
    public CompletableFuture<List<ICommandData>> getCommandsOfControlStream(ICommandStreamInfo commandStreamInfo, ResourceFormat format, String query)
    {
        query = query == null ? "" : query;

        return sendGetRequest(endpoint.resolve(CONTROLS_COLLECTION + "/" + commandStreamInfo.getID() + "/" + COMMANDS_COLLECTION + query), format, body ->
                getCollectionItems(body, itemBody -> {
                    try
                    {
                        CommandHandler.CommandHandlerContextData contextData = new CommandHandler.CommandHandlerContextData();
                        contextData.dsInfo = commandStreamInfo;

                        var ctx = new RequestContext(itemBody);
                        ctx.setData(contextData);
                        ctx.setFormat(ResourceFormat.OM_JSON);

                        var binding = new CommandBindingJson(ctx, null, true, null);
                        return binding.deserialize();

                    }
                    catch (IOException e)
                    {
                        throw new CompletionException(e);
                    }
                })
        );
    }

    /**
     * Get the command by its local identifier.
     *
     * @param commandId         The local identifier of the command
     * @param commandStreamInfo The command stream description
     * @return The command
     */
    public CompletableFuture<ICommandData> getCommandById(String commandId, ResourceFormat format, ICommandStreamInfo commandStreamInfo)
    {
        return sendGetRequest(endpoint.resolve(CONTROLS_COLLECTION + "/" + commandStreamInfo.getID() + "/" + COMMANDS_COLLECTION + "/" + commandId), format, body -> {
            try
            {
                CommandHandler.CommandHandlerContextData contextData = new CommandHandler.CommandHandlerContextData();
                contextData.dsInfo = commandStreamInfo;

                var ctx = new RequestContext(body);
                ctx.setData(contextData);
                ctx.setFormat(ResourceFormat.OM_JSON);

                var binding = new CommandBindingJson(ctx, null, true, null);
                return binding.deserialize();

            }
            catch (IOException e)
            {
                throw new CompletionException(e);
            }
        });
    }

    /**
     * Add a new command to an existing control stream.
     *
     * @param commandStreamInfo The command stream description
     * @param commandData       The command to be added
     * @return The local identifier of the new command
     */
    public CompletableFuture<String> sendCommand(ICommandStreamInfo commandStreamInfo, ICommandData commandData)
    {
        try
        {
            CommandHandler.CommandHandlerContextData contextData = new CommandHandler.CommandHandlerContextData();
            contextData.dsInfo = commandStreamInfo;

            var buffer = new ByteArrayOutputStream();
            var ctx = new RequestContext(buffer);
            ctx.setData(contextData);
            ctx.setFormat(ResourceFormat.JSON);

            var binding = new CommandBindingJson(ctx, null, false, null);
            binding.serialize(null, commandData, false);

            return sendPostRequest(
                    endpoint.resolve(CONTROLS_COLLECTION + "/" + commandStreamInfo.getID() + "/" + COMMANDS_COLLECTION),
                    ctx.getFormat(),
                    buffer.toByteArray());
        }
        catch (IOException e)
        {
            throw new IllegalStateException(BINDING_ERROR, e);
        }
    }
    
    
    /*----------------*/
    /* Helper Methods */
    /*----------------*/

    protected <T> CompletableFuture<T> sendGetRequest(URI collectionUri, ResourceFormat format, Function<InputStream, T> bodyMapper)
    {
        if (!isHttpClientAvailable)
            return sendGetRequestFallback(collectionUri, format, bodyMapper);

        var req = HttpRequest.newBuilder()
                .uri(collectionUri)
                .GET()
                .header(HttpHeaders.ACCEPT, format.getMimeType())
                .build();

        BodyHandler<T> bodyHandler = resp -> {
            BodySubscriber<byte[]> upstream = BodySubscribers.ofByteArray();
            return BodySubscribers.mapping(upstream, body -> {
                var is = new ByteArrayInputStream(body);
                return bodyMapper.apply(is);
            });
        };

        return http.sendAsync(req, bodyHandler)
                .thenApply(resp -> {
                    if (resp.statusCode() == 200)
                        return resp.body();
                    else
                        throw new CompletionException("HTTP error " + resp.statusCode(), null);
                });
    }


    /**
     * Fallback method for sending requests using HttpURLConnection.
     * This is used when HttpClient is not available (e.g., on Android).
     */
    protected <T> CompletableFuture<T> sendGetRequestFallback(URI collectionUri, ResourceFormat format, Function<InputStream, T> bodyMapper)
    {
        return CompletableFuture.supplyAsync(() -> {
            HttpURLConnection connection = null;
            try {
                if (authenticator != null)
                    Authenticator.setDefault(authenticator);

                URL url = collectionUri.toURL();
                connection = (HttpURLConnection) url.openConnection();
                connection.setRequestMethod("GET");
                connection.setRequestProperty(HttpHeaders.ACCEPT, format.getMimeType());

                int responseCode = connection.getResponseCode();
                if (responseCode == 200) {
                    try (InputStream is = connection.getInputStream()) {
                        return bodyMapper.apply(is);
                    }
                } else {
                    throw new CompletionException("HTTP error " + responseCode, null);
                }
            } catch (IOException e) {
                throw new CompletionException(e);
            } finally {
                if (connection != null) {
                    connection.disconnect();
                }
            }
        });
    }


    protected CompletableFuture<String> sendPostRequest(URI collectionUri, ResourceFormat format, byte[] body)
    {
        if (!isHttpClientAvailable)
            return sendPostRequestFallback(collectionUri, format, body);

        var req = HttpRequest.newBuilder()
                .uri(collectionUri)
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .header(HttpHeaders.ACCEPT, ResourceFormat.JSON.getMimeType())
                .header(HttpHeaders.CONTENT_TYPE, format.getMimeType())
                .build();

        return http.sendAsync(req, BodyHandlers.ofString())
                .thenApply(resp -> {
                    if (resp.statusCode() == 201 || resp.statusCode() == 303) {
                        var location = resp.headers()
                                .firstValue(HttpHeaders.LOCATION)
                                .orElseThrow(() -> new IllegalStateException("Missing Location header in response"));
                        return location.substring(location.lastIndexOf('/') + 1);
                    } else if (resp.statusCode() == 200)
                        // Commands have a status code of 200 but no Location header
                        return resp.body();
                    else
                        throw new CompletionException(resp.body(), null);
                });
    }


    /**
     * Fallback method for sending requests using HttpURLConnection.
     * This is used when HttpClient is not available (e.g., on Android).
     */
    protected CompletableFuture<String> sendPostRequestFallback(URI collectionUri, ResourceFormat format, byte[] body)
    {
        return CompletableFuture.supplyAsync(() -> {
            HttpURLConnection connection = null;
            try {
                if (authenticator != null)
                    Authenticator.setDefault(authenticator);

                URL url = collectionUri.toURL();
                connection = (HttpURLConnection) url.openConnection();
                connection.setRequestMethod("POST");
                connection.setRequestProperty(HttpHeaders.ACCEPT, ResourceFormat.JSON.getMimeType());
                connection.setRequestProperty(HttpHeaders.CONTENT_TYPE, format.getMimeType());
                connection.setDoOutput(true);

                try (OutputStream os = connection.getOutputStream()) {
                    os.write(body);
                }

                int responseCode = connection.getResponseCode();
                if (responseCode == 201 || responseCode == 303) {
                    String location = connection.getHeaderField(HttpHeaders.LOCATION);
                    if (location == null) {
                        throw new IllegalStateException("Missing Location header in response.");
                    }
                    return location.substring(location.lastIndexOf('/') + 1);
                } else if (responseCode == 200)
                    // Commands have a status code of 200 but no Location header
                    try (InputStream is = connection.getInputStream())
                    {
                        return new String(is.readAllBytes());
                    }
                else
                    throw new CompletionException(connection.getResponseMessage(), null);
            } catch (IOException e) {
                throw new CompletionException(e);
            } finally {
                if (connection != null) {
                    connection.disconnect();
                }
            }
        });
    }


    protected CompletableFuture<Integer> sendPutRequest(URI collectionUri, ResourceFormat format, byte[] body)
    {
        if (!isHttpClientAvailable)
            return sendPutRequestFallback(collectionUri, format, body);

        var req = HttpRequest.newBuilder()
                .uri(collectionUri)
                .PUT(HttpRequest.BodyPublishers.ofByteArray(body))
                .header(HttpHeaders.ACCEPT, ResourceFormat.JSON.getMimeType())
                .header(HttpHeaders.CONTENT_TYPE, format.getMimeType())
                .build();

        return http.sendAsync(req, BodyHandlers.ofString())
                .thenApply(HttpResponse::statusCode);
    }


    /**
     * Fallback method for sending requests using HttpURLConnection.
     * This is used when HttpClient is not available (e.g., on Android).
     */
    protected CompletableFuture<Integer> sendPutRequestFallback(URI collectionUri, ResourceFormat format, byte[] body)
    {
        return CompletableFuture.supplyAsync(() -> {
            HttpURLConnection connection = null;
            try {
                if (authenticator != null)
                    Authenticator.setDefault(authenticator);

                URL url = collectionUri.toURL();
                connection = (HttpURLConnection) url.openConnection();
                connection.setRequestMethod("PUT");
                connection.setRequestProperty(HttpHeaders.ACCEPT, ResourceFormat.JSON.getMimeType());
                connection.setRequestProperty(HttpHeaders.CONTENT_TYPE, format.getMimeType());
                connection.setDoOutput(true);

                try (OutputStream os = connection.getOutputStream()) {
                    os.write(body);
                }

                return connection.getResponseCode();
            } catch (IOException e) {
                throw new CompletionException(e);
            } finally {
                if (connection != null) {
                    connection.disconnect();
                }
            }
        });
    }


    protected CompletableFuture<Set<String>> sendBatchPostRequest(URI collectionUri, ResourceFormat format, byte[] body)
    {
        if (!isHttpClientAvailable)
            return sendBatchPostRequestFallback(collectionUri, format, body);

        var req = HttpRequest.newBuilder()
                .uri(collectionUri)
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .header(HttpHeaders.CONTENT_TYPE, format.getMimeType())
                .build();

        return http.sendAsync(req, BodyHandlers.ofString())
                .thenApply(Lambdas.checked(resp -> {
                    if (resp.statusCode() == 201 || resp.statusCode() == 303) {
                        var idList = new LinkedHashSet<String>();
                        try (JsonReader reader = new JsonReader(new StringReader(resp.body()))) {
                            reader.beginArray();
                            while (reader.hasNext()) {
                                var uri = reader.nextString();
                                idList.add(uri.substring(uri.lastIndexOf('/') + 1));
                            }
                            reader.endArray();
                        }
                        return idList;
                    } else
                        throw new ResourceParseException(resp.body());
                }));
    }


    /**
     * Fallback method for sending requests using HttpURLConnection.
     * This is used when HttpClient is not available (e.g., on Android).
     */
    protected CompletableFuture<Set<String>> sendBatchPostRequestFallback(URI collectionUri, ResourceFormat format, byte[] body)
    {
        return CompletableFuture.supplyAsync(() -> {
            HttpURLConnection connection = null;
            try {
                if (authenticator != null) {
                    Authenticator.setDefault(authenticator);
                }

                URL url = collectionUri.toURL();
                connection = (HttpURLConnection) url.openConnection();
                connection.setRequestMethod("POST");
                connection.setRequestProperty(HttpHeaders.CONTENT_TYPE, format.getMimeType());
                connection.setDoOutput(true);

                try (OutputStream os = connection.getOutputStream()) {
                    os.write(body);
                }

                int responseCode = connection.getResponseCode();
                if (responseCode == 201 || responseCode == 303) {
                    Set<String> idList = new LinkedHashSet<>();
                    try (InputStream is = connection.getInputStream();
                         JsonReader reader = new JsonReader(new InputStreamReader(is))) {
                        reader.beginArray();
                        while (reader.hasNext()) {
                            String uri = reader.nextString();
                            idList.add(uri.substring(uri.lastIndexOf('/') + 1));
                        }
                        reader.endArray();
                    }
                    return idList;
                } else {
                    throw new ResourceParseException(connection.getResponseMessage());
                }
            } catch (IOException e) {
                throw new CompletionException(e);
            } finally {
                if (connection != null) {
                    connection.disconnect();
                }
            }
        });
    }


    protected CompletableFuture<Integer> sendDeleteRequest(URI collectionUri)
    {
        if (!isHttpClientAvailable)
            return sendDeleteRequestFallback(collectionUri);

        var req = HttpRequest.newBuilder()
                .uri(collectionUri)
                .DELETE()
                .header(HttpHeaders.ACCEPT, ResourceFormat.JSON.getMimeType())
                .build();

        return http.sendAsync(req, BodyHandlers.ofString())
                .thenApply(HttpResponse::statusCode);
    }


    /**
     * Fallback method for sending requests using HttpURLConnection.
     * This is used when HttpClient is not available (e.g., on Android).
     */
    protected CompletableFuture<Integer> sendDeleteRequestFallback(URI collectionUri)
    {
        return CompletableFuture.supplyAsync(() -> {
            HttpURLConnection connection = null;
            try
            {
                URL url = collectionUri.toURL();
                connection = (HttpURLConnection) url.openConnection();
                if (authenticator != null)
                {
                    Authenticator.setDefault(authenticator);
                }
                connection.setRequestMethod("DELETE");
                connection.setRequestProperty(HttpHeaders.ACCEPT, ResourceFormat.JSON.getMimeType());

                return connection.getResponseCode();
            }
            catch (IOException e)
            {
                throw new CompletionException(e);
            }
            finally
            {
                if (connection != null)
                {
                    connection.disconnect();
                }
            }
        });
    }


    protected void skipToCollectionItems(JsonReader reader) throws IOException
    {
        // skip to array of collection items
        reader.beginObject();
        while (reader.hasNext())
        {
            var name = reader.nextName();
            if ("items".equals(name) || "features".equals(name))
                break;
            else
                reader.skipValue();
        }
    }


    /**
     * Get the items from a collection in the body of the response.
     *
     * @param body   The input stream representing the entire response body
     * @param mapper A function to map an input stream representing an individual item to the desired type
     * @param <T>    The type of the items in the collection
     * @return A list of items in the collection, mapped to the desired type
     */
    protected <T> List<T> getCollectionItems(InputStream body, Function<InputStream, T> mapper)
    {
        try
        {
            JsonObject bodyJson = JsonParser.parseReader(new InputStreamReader(body)).getAsJsonObject();
            JsonArray arrayItems = bodyJson.getAsJsonArray("items");

            List<T> collectionItems = new ArrayList<>();
            for (JsonElement item : arrayItems)
            {
                var ctx = new RequestContext(new ByteArrayInputStream(item.toString().getBytes()));
                collectionItems.add(mapper.apply(ctx.getInputStream()));
            }
            return collectionItems;
        }
        catch (IOException e)
        {
            throw new CompletionException(e);
        }
    }


    /* Builder stuff */

    public static ConSysApiClientBuilder newBuilder(String endpoint)
    {
        Asserts.checkNotNull(endpoint, "endpoint");
        return new ConSysApiClientBuilder(endpoint);
    }


    public static class ConSysApiClientBuilder extends BaseBuilder<ConSysApiClient>
    {
        HttpClient.Builder httpClientBuilder;

        ConSysApiClientBuilder(String endpoint)
        {
            this.instance = new ConSysApiClient();
            if (isHttpClientAvailable)
                this.httpClientBuilder = HttpClient.newBuilder();

            try
            {
                if (!endpoint.endsWith("/"))
                    endpoint += "/";
                instance.endpoint = new URI(endpoint);
            }
            catch (URISyntaxException e)
            {
                throw new IllegalArgumentException("Invalid URI " + endpoint);
            }
        }


        public ConSysApiClientBuilder useHttpClient(HttpClient http)
        {
            instance.http = http;
            return this;
        }


        public ConSysApiClientBuilder simpleAuth(String user, char[] password)
        {
            if (!Strings.isNullOrEmpty(user))
            {
                var finalPwd = password != null ? password : new char[0];
                instance.authenticator = new Authenticator() {
                    @Override
                    protected PasswordAuthentication getPasswordAuthentication() {
                        return new PasswordAuthentication(user, finalPwd);
                    }
                };

                if (isHttpClientAvailable)
                    httpClientBuilder.authenticator(instance.authenticator);
            }

            return this;
        }


        @Override
        public ConSysApiClient build()
        {
            if (isHttpClientAvailable && instance.http == null)
                instance.http = httpClientBuilder.build();
            return instance;
        }
    }
}
