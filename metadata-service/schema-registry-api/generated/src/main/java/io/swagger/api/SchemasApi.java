/**
 * NOTE: This class is auto generated by the swagger code generator program (3.0.33).
 * https://github.com/swagger-api/swagger-codegen Do not edit the class manually.
 */
package io.swagger.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.datahubproject.schema_registry.openapi.generated.ErrorMessage;
import io.datahubproject.schema_registry.openapi.generated.Schema;
import io.datahubproject.schema_registry.openapi.generated.SchemaString;
import io.datahubproject.schema_registry.openapi.generated.SubjectVersion;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

@jakarta.annotation.Generated(
    value = "io.swagger.codegen.v3.generators.java.SpringCodegen",
    date = "2022-12-20T16:52:36.517693Z[Europe/Lisbon]")
@Validated
public interface SchemasApi {

  Logger log = LoggerFactory.getLogger(SchemasApi.class);

  default Optional<ObjectMapper> getObjectMapper() {
    return Optional.empty();
  }

  default Optional<HttpServletRequest> getRequest() {
    return Optional.empty();
  }

  default Optional<String> getAcceptHeader() {
    return getRequest().map(r -> r.getHeader("Accept"));
  }

  @Operation(
      summary = "Get schema string by ID",
      description = "Retrieves the schema string identified by the input ID.",
      tags = {"Schemas (v1)"})
  @ApiResponses(
      value = {
        @ApiResponse(
            responseCode = "200",
            description = "The schema string.",
            content =
                @Content(
                    mediaType = "application/vnd.schemaregistry.v1+json",
                    schema =
                        @io.swagger.v3.oas.annotations.media.Schema(
                            implementation = SchemaString.class))),
        @ApiResponse(
            responseCode = "404",
            description = "Not Found. Error code 40403 indicates schema not found.",
            content =
                @Content(
                    mediaType = "application/vnd.schemaregistry.v1+json",
                    schema =
                        @io.swagger.v3.oas.annotations.media.Schema(
                            implementation = ErrorMessage.class))),
        @ApiResponse(
            responseCode = "500",
            description =
                "Internal Server Error. Error code 50001 indicates a failure in the backend data store.",
            content =
                @Content(
                    mediaType = "application/vnd.schemaregistry.v1+json",
                    schema =
                        @io.swagger.v3.oas.annotations.media.Schema(
                            implementation = ErrorMessage.class)))
      })
  @RequestMapping(
      value = "/schemas/ids/{id}",
      produces = {
        "application/vnd.schemaregistry.v1+json",
        "application/vnd.schemaregistry+json; qs=0.9",
        "application/json; qs=0.5"
      },
      method = RequestMethod.GET)
  default ResponseEntity<SchemaString> getSchema(
      @Parameter(
              in = ParameterIn.PATH,
              description = "Globally unique identifier of the schema",
              required = true,
              schema = @io.swagger.v3.oas.annotations.media.Schema())
          @PathVariable("id")
          Integer id,
      @Parameter(
              in = ParameterIn.QUERY,
              description = "Name of the subject",
              schema = @io.swagger.v3.oas.annotations.media.Schema())
          @Valid
          @RequestParam(value = "subject", required = false)
          String subject,
      @Parameter(
              in = ParameterIn.QUERY,
              description = "Desired output format, dependent on schema type",
              schema = @io.swagger.v3.oas.annotations.media.Schema())
          @Valid
          @RequestParam(value = "format", required = false)
          String format,
      @Parameter(
              in = ParameterIn.QUERY,
              description = "Whether to fetch the maximum schema identifier that exists",
              schema = @io.swagger.v3.oas.annotations.media.Schema(defaultValue = "false"))
          @Valid
          @RequestParam(value = "fetchMaxId", required = false, defaultValue = "false")
          Boolean fetchMaxId) {
    if (getObjectMapper().isPresent() && getAcceptHeader().isPresent()) {
      if (getAcceptHeader().get().contains("application/json")) {
        try {
          return new ResponseEntity<>(
              getObjectMapper()
                  .get()
                  .readValue(
                      "{\n  \"schema\" : \"{\"schema\": \"{\"type\": \"string\"}\"}\",\n  \"maxId\" : 1,\n  \"references\" : [ {\n    \"subject\" : \"User\",\n    \"name\" : \"io.confluent.kafka.example.User\",\n    \"version\" : 1\n  }, {\n    \"subject\" : \"User\",\n    \"name\" : \"io.confluent.kafka.example.User\",\n    \"version\" : 1\n  } ],\n  \"schemaType\" : \"AVRO\"\n}",
                      SchemaString.class),
              HttpStatus.NOT_IMPLEMENTED);
        } catch (IOException e) {
          log.error("Couldn't serialize response for content type application/json", e);
          return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
      }
    } else {
      log.warn(
          "ObjectMapper or HttpServletRequest not configured in default SchemasApi interface so no example is generated");
    }
    return new ResponseEntity<>(HttpStatus.NOT_IMPLEMENTED);
  }

  @Operation(
      summary = "Get schema by ID",
      description = "Retrieves the schema identified by the input ID.",
      tags = {"Schemas (v1)"})
  @ApiResponses(
      value = {
        @ApiResponse(
            responseCode = "200",
            description = "Raw schema string.",
            content =
                @Content(
                    mediaType = "application/vnd.schemaregistry.v1+json",
                    schema =
                        @io.swagger.v3.oas.annotations.media.Schema(
                            implementation = String.class))),
        @ApiResponse(
            responseCode = "404",
            description = "Not Found. Error code 40403 indicates schema not found.",
            content =
                @Content(
                    mediaType = "application/vnd.schemaregistry.v1+json",
                    schema =
                        @io.swagger.v3.oas.annotations.media.Schema(
                            implementation = ErrorMessage.class))),
        @ApiResponse(
            responseCode = "500",
            description =
                "Internal Server Error. Error code 50001 indicates a failure in the backend data store.",
            content =
                @Content(
                    mediaType = "application/vnd.schemaregistry.v1+json",
                    schema =
                        @io.swagger.v3.oas.annotations.media.Schema(
                            implementation = ErrorMessage.class)))
      })
  @RequestMapping(
      value = "/schemas/ids/{id}/schema",
      produces = {
        "application/vnd.schemaregistry.v1+json",
        "application/vnd.schemaregistry+json; qs=0.9",
        "application/json; qs=0.5"
      },
      method = RequestMethod.GET)
  default ResponseEntity<String> getSchemaOnly(
      @Parameter(
              in = ParameterIn.PATH,
              description = "Globally unique identifier of the schema",
              required = true,
              schema = @io.swagger.v3.oas.annotations.media.Schema())
          @PathVariable("id")
          Integer id,
      @Parameter(
              in = ParameterIn.QUERY,
              description = "Name of the subject",
              schema = @io.swagger.v3.oas.annotations.media.Schema())
          @Valid
          @RequestParam(value = "subject", required = false)
          String subject,
      @Parameter(
              in = ParameterIn.QUERY,
              description = "Desired output format, dependent on schema type",
              schema = @io.swagger.v3.oas.annotations.media.Schema())
          @Valid
          @RequestParam(value = "format", required = false)
          String format) {
    if (getObjectMapper().isPresent() && getAcceptHeader().isPresent()) {
      if (getAcceptHeader().get().contains("application/json")) {
        try {
          return new ResponseEntity<>(
              getObjectMapper()
                  .get()
                  .readValue("\"{\"schema\": \"{\"type\": \"string\"}\"}\"", String.class),
              HttpStatus.NOT_IMPLEMENTED);
        } catch (IOException e) {
          log.error("Couldn't serialize response for content type application/json", e);
          return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
      }
    } else {
      log.warn(
          "ObjectMapper or HttpServletRequest not configured in default SchemasApi interface so no example is generated");
    }
    return new ResponseEntity<>(HttpStatus.NOT_IMPLEMENTED);
  }

  @Operation(
      summary = "List supported schema types",
      description = "Retrieve the schema types supported by this registry.",
      tags = {"Schemas (v1)"})
  @ApiResponses(
      value = {
        @ApiResponse(
            responseCode = "200",
            description = "List of supported schema types.",
            content =
                @Content(
                    mediaType = "application/vnd.schemaregistry.v1+json",
                    array =
                        @ArraySchema(
                            schema =
                                @io.swagger.v3.oas.annotations.media.Schema(
                                    implementation = String.class)))),
        @ApiResponse(
            responseCode = "500",
            description =
                "Internal Server Error. Error code 50001 indicates a failure in the backend data store.",
            content =
                @Content(
                    mediaType = "application/vnd.schemaregistry.v1+json",
                    schema =
                        @io.swagger.v3.oas.annotations.media.Schema(
                            implementation = ErrorMessage.class)))
      })
  @RequestMapping(
      value = "/schemas/types",
      produces = {
        "application/vnd.schemaregistry.v1+json",
        "application/vnd.schemaregistry+json; qs=0.9",
        "application/json; qs=0.5"
      },
      method = RequestMethod.GET)
  default ResponseEntity<List<String>> getSchemaTypes() {
    if (getObjectMapper().isPresent() && getAcceptHeader().isPresent()) {
      if (getAcceptHeader().get().contains("application/json")) {
        try {
          return new ResponseEntity<>(
              getObjectMapper().get().readValue("[ \"AVRO\", \"AVRO\" ]", List.class),
              HttpStatus.NOT_IMPLEMENTED);
        } catch (IOException e) {
          log.error("Couldn't serialize response for content type application/json", e);
          return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
      }
    } else {
      log.warn(
          "ObjectMapper or HttpServletRequest not configured in default SchemasApi interface so no example is generated");
    }
    return new ResponseEntity<>(HttpStatus.NOT_IMPLEMENTED);
  }

  @Operation(
      summary = "List schemas",
      description = "Get the schemas matching the specified parameters.",
      tags = {"Schemas (v1)"})
  @ApiResponses(
      value = {
        @ApiResponse(
            responseCode = "200",
            description = "List of schemas matching the specified parameters.",
            content =
                @Content(
                    mediaType = "application/vnd.schemaregistry.v1+json",
                    array =
                        @ArraySchema(
                            schema =
                                @io.swagger.v3.oas.annotations.media.Schema(
                                    implementation = Schema.class)))),
        @ApiResponse(
            responseCode = "500",
            description =
                "Internal Server Error. Error code 50001 indicates a failure in the backend data store.",
            content =
                @Content(
                    mediaType = "application/vnd.schemaregistry.v1+json",
                    schema =
                        @io.swagger.v3.oas.annotations.media.Schema(
                            implementation = ErrorMessage.class)))
      })
  @RequestMapping(
      value = "/schemas",
      produces = {
        "application/vnd.schemaregistry.v1+json",
        "application/vnd.schemaregistry+json; qs=0.9",
        "application/json; qs=0.5"
      },
      method = RequestMethod.GET)
  default ResponseEntity<List<Schema>> getSchemas(
      @Parameter(
              in = ParameterIn.QUERY,
              description = "Filters results by the respective subject prefix",
              schema = @io.swagger.v3.oas.annotations.media.Schema())
          @Valid
          @RequestParam(value = "subjectPrefix", required = false)
          String subjectPrefix,
      @Parameter(
              in = ParameterIn.QUERY,
              description = "Whether to return soft deleted schemas",
              schema = @io.swagger.v3.oas.annotations.media.Schema(defaultValue = "false"))
          @Valid
          @RequestParam(value = "deleted", required = false, defaultValue = "false")
          Boolean deleted,
      @Parameter(
              in = ParameterIn.QUERY,
              description =
                  "Whether to return latest schema versions only for each matching subject",
              schema = @io.swagger.v3.oas.annotations.media.Schema(defaultValue = "false"))
          @Valid
          @RequestParam(value = "latestOnly", required = false, defaultValue = "false")
          Boolean latestOnly,
      @Parameter(
              in = ParameterIn.QUERY,
              description = "Pagination offset for results",
              schema = @io.swagger.v3.oas.annotations.media.Schema(defaultValue = "0"))
          @Valid
          @RequestParam(value = "offset", required = false, defaultValue = "0")
          Integer offset,
      @Parameter(
              in = ParameterIn.QUERY,
              description = "Pagination size for results. Ignored if negative",
              schema = @io.swagger.v3.oas.annotations.media.Schema(defaultValue = "-1"))
          @Valid
          @RequestParam(value = "limit", required = false, defaultValue = "-1")
          Integer limit) {
    if (getObjectMapper().isPresent() && getAcceptHeader().isPresent()) {
      if (getAcceptHeader().get().contains("application/json")) {
        try {
          return new ResponseEntity<>(
              getObjectMapper()
                  .get()
                  .readValue(
                      "[ {\n  \"schema\" : \"{\"schema\": \"{\"type\": \"string\"}\"}\",\n  \"references\" : [ {\n    \"subject\" : \"User\",\n    \"name\" : \"io.confluent.kafka.example.User\",\n    \"version\" : 1\n  }, {\n    \"subject\" : \"User\",\n    \"name\" : \"io.confluent.kafka.example.User\",\n    \"version\" : 1\n  } ],\n  \"subject\" : \"User\",\n  \"schemaType\" : \"AVRO\",\n  \"id\" : 100001,\n  \"version\" : 1\n}, {\n  \"schema\" : \"{\"schema\": \"{\"type\": \"string\"}\"}\",\n  \"references\" : [ {\n    \"subject\" : \"User\",\n    \"name\" : \"io.confluent.kafka.example.User\",\n    \"version\" : 1\n  }, {\n    \"subject\" : \"User\",\n    \"name\" : \"io.confluent.kafka.example.User\",\n    \"version\" : 1\n  } ],\n  \"subject\" : \"User\",\n  \"schemaType\" : \"AVRO\",\n  \"id\" : 100001,\n  \"version\" : 1\n} ]",
                      List.class),
              HttpStatus.NOT_IMPLEMENTED);
        } catch (IOException e) {
          log.error("Couldn't serialize response for content type application/json", e);
          return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
      }
    } else {
      log.warn(
          "ObjectMapper or HttpServletRequest not configured in default SchemasApi interface so no example is generated");
    }
    return new ResponseEntity<>(HttpStatus.NOT_IMPLEMENTED);
  }

  @Operation(
      summary = "List subjects associated to schema ID",
      description = "Retrieves all the subjects associated with a particular schema ID.",
      tags = {"Schemas (v1)"})
  @ApiResponses(
      value = {
        @ApiResponse(
            responseCode = "200",
            description = "List of subjects matching the specified parameters.",
            content =
                @Content(
                    mediaType = "application/vnd.schemaregistry.v1+json",
                    array =
                        @ArraySchema(
                            schema =
                                @io.swagger.v3.oas.annotations.media.Schema(
                                    implementation = String.class)))),
        @ApiResponse(
            responseCode = "404",
            description = "Not Found. Error code 40403 indicates schema not found.",
            content =
                @Content(
                    mediaType = "application/vnd.schemaregistry.v1+json",
                    schema =
                        @io.swagger.v3.oas.annotations.media.Schema(
                            implementation = ErrorMessage.class))),
        @ApiResponse(
            responseCode = "500",
            description =
                "Internal Server Error. Error code 50001 indicates a failure in the backend data store.",
            content =
                @Content(
                    mediaType = "application/vnd.schemaregistry.v1+json",
                    schema =
                        @io.swagger.v3.oas.annotations.media.Schema(
                            implementation = ErrorMessage.class)))
      })
  @RequestMapping(
      value = "/schemas/ids/{id}/subjects",
      produces = {
        "application/vnd.schemaregistry.v1+json",
        "application/vnd.schemaregistry+json; qs=0.9",
        "application/json; qs=0.5"
      },
      method = RequestMethod.GET)
  default ResponseEntity<List<String>> getSubjects(
      @Parameter(
              in = ParameterIn.PATH,
              description = "Globally unique identifier of the schema",
              required = true,
              schema = @io.swagger.v3.oas.annotations.media.Schema())
          @PathVariable("id")
          Integer id,
      @Parameter(
              in = ParameterIn.QUERY,
              description = "Filters results by the respective subject",
              schema = @io.swagger.v3.oas.annotations.media.Schema())
          @Valid
          @RequestParam(value = "subject", required = false)
          String subject,
      @Parameter(
              in = ParameterIn.QUERY,
              description = "Whether to include subjects where the schema was deleted",
              schema = @io.swagger.v3.oas.annotations.media.Schema())
          @Valid
          @RequestParam(value = "deleted", required = false)
          Boolean deleted) {
    if (getObjectMapper().isPresent() && getAcceptHeader().isPresent()) {
      if (getAcceptHeader().get().contains("application/json")) {
        try {
          return new ResponseEntity<>(
              getObjectMapper().get().readValue("[ \"User\", \"User\" ]", List.class),
              HttpStatus.NOT_IMPLEMENTED);
        } catch (IOException e) {
          log.error("Couldn't serialize response for content type application/json", e);
          return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
      }
    } else {
      log.warn(
          "ObjectMapper or HttpServletRequest not configured in default SchemasApi interface so no example is generated");
    }
    return new ResponseEntity<>(HttpStatus.NOT_IMPLEMENTED);
  }

  @Operation(
      summary = "List subject-versions associated to schema ID",
      description = "Get all the subject-version pairs associated with the input ID.",
      tags = {"Schemas (v1)"})
  @ApiResponses(
      value = {
        @ApiResponse(
            responseCode = "200",
            description = "List of subject versions matching the specified parameters.",
            content =
                @Content(
                    mediaType = "application/vnd.schemaregistry.v1+json",
                    array =
                        @ArraySchema(
                            schema =
                                @io.swagger.v3.oas.annotations.media.Schema(
                                    implementation = SubjectVersion.class)))),
        @ApiResponse(
            responseCode = "404",
            description = "Not Found. Error code 40403 indicates schema not found.",
            content =
                @Content(
                    mediaType = "application/vnd.schemaregistry.v1+json",
                    schema =
                        @io.swagger.v3.oas.annotations.media.Schema(
                            implementation = ErrorMessage.class))),
        @ApiResponse(
            responseCode = "500",
            description =
                "Internal Server Error. Error code 50001 indicates a failure in the backend data store.",
            content =
                @Content(
                    mediaType = "application/vnd.schemaregistry.v1+json",
                    schema =
                        @io.swagger.v3.oas.annotations.media.Schema(
                            implementation = ErrorMessage.class)))
      })
  @RequestMapping(
      value = "/schemas/ids/{id}/versions",
      produces = {
        "application/vnd.schemaregistry.v1+json",
        "application/vnd.schemaregistry+json; qs=0.9",
        "application/json; qs=0.5"
      },
      method = RequestMethod.GET)
  default ResponseEntity<List<SubjectVersion>> getVersions(
      @Parameter(
              in = ParameterIn.PATH,
              description = "Globally unique identifier of the schema",
              required = true,
              schema = @io.swagger.v3.oas.annotations.media.Schema())
          @PathVariable("id")
          Integer id,
      @Parameter(
              in = ParameterIn.QUERY,
              description = "Filters results by the respective subject",
              schema = @io.swagger.v3.oas.annotations.media.Schema())
          @Valid
          @RequestParam(value = "subject", required = false)
          String subject,
      @Parameter(
              in = ParameterIn.QUERY,
              description = "Whether to include subject versions where the schema was deleted",
              schema = @io.swagger.v3.oas.annotations.media.Schema())
          @Valid
          @RequestParam(value = "deleted", required = false)
          Boolean deleted) {
    if (getObjectMapper().isPresent() && getAcceptHeader().isPresent()) {
      if (getAcceptHeader().get().contains("application/json")) {
        try {
          return new ResponseEntity<>(
              getObjectMapper()
                  .get()
                  .readValue(
                      "[ {\n  \"subject\" : \"User\",\n  \"version\" : 1\n}, {\n  \"subject\" : \"User\",\n  \"version\" : 1\n} ]",
                      List.class),
              HttpStatus.NOT_IMPLEMENTED);
        } catch (IOException e) {
          log.error("Couldn't serialize response for content type application/json", e);
          return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
      }
    } else {
      log.warn(
          "ObjectMapper or HttpServletRequest not configured in default SchemasApi interface so no example is generated");
    }
    return new ResponseEntity<>(HttpStatus.NOT_IMPLEMENTED);
  }
}
