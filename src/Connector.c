/* $CORTO_GENERATED
 *
 * Connector.c
 *
 * Only code written between the begin and end tags will be preserved
 * when the file is regenerated.
 */

#include "corto/net/cities/cities.h"

corto_int16 _cities_Connector_construct(
    cities_Connector this)
{
/* $begin(corto/net/cities/Connector/construct) */
    if (corto_replicator_setContentType(this, "text/json")) {
        goto error;
    }

    return corto_replicator_construct(this);
error:
    return -1;
/* $end */
}

/* $header(corto/net/cities/Connector/onRequest) */
typedef struct cities_onRequest_data {
    corto_file file;
    corto_string parent;
    JSON_Value *value;
    JSON_Object *country;
    corto_result result;
    corto_bool setContent;
    corto_int32 i;
    corto_string json;
    char buff[1024];
} cities_onRequest_data;

int cities_iterHasNext(corto_iter *iter) {
    cities_onRequest_data *data = iter->udata;
    corto_bool found = FALSE;

    while (!found) {
        char *json = corto_fileReadLine(data->file, data->buff, sizeof(data->buff));
        if (json) {
            if (strstr(json, data->parent)) {
                data->value = json_parse_string(json);
                if (data->value) {
                    JSON_Object *object = json_value_get_object(data->value);
                    if (object) {
                        corto_string parent = (corto_string)json_object_dotget_string(object, "country");
                        if (!strcmp(parent, data->parent)) {
                            found = TRUE;
                        }
                    }
                }
            }
        } else {
            break;
        }
    }

    return found;
}

void* cities_iterNext(corto_iter *iter) {
    cities_onRequest_data *data = iter->udata;

    if (data->value) {
        JSON_Object *object = json_value_get_object(data->value);
        if (object) {
            corto_asprintf(&data->result.id, "%d",
              (corto_uint32)json_object_dotget_number(object, "_id"));
            data->result.name = (corto_string)json_object_dotget_string(object, "name");

            data->result.parent = ".";
            data->result.type = "/corto/net/cities/City";

            if (data->setContent) {
                JSON_Object *coord = json_object_dotget_object(object, "coord");
                corto_asprintf((corto_string*)&data->result.value,
                    "{\"name\":\"%s\", \"coord\":{\"latitude\":%f,\"longitude\":%f}}",
                      data->result.name,
                      json_object_dotget_number(coord, "lat"),
                      json_object_dotget_number(coord, "lon"));
            } else {
                data->result.value = 0;
            }
        }
    }

    return &data->result;
}

static int cities_iterCountriesHasNext(corto_resultIter *iter) {
    cities_onRequest_data *data = iter->udata;

    JSON_Array *array = json_value_get_array(data->value);
    data->country = json_array_get_object(array, data->i);

    return data->country != NULL;
}

static void* cities_iterCountriesNext(corto_resultIter *iter) {
    cities_onRequest_data *data = iter->udata;

    data->result.id = (corto_string)json_object_dotget_string(data->country, "alpha-2");
    data->result.name = (corto_string)json_object_dotget_string(data->country, "name");
    data->result.parent = ".";
    data->result.type = "/corto/net/cities/Country";
    if (data->setContent) {
        corto_asprintf((corto_string*)&data->result.value,
            "{\"name\":\"%s\", \"region\":\"%s\",\"subregion\":\"%s\"}",
              data->result.name,
              json_object_dotget_string(data->country, "region"),
              json_object_dotget_string(data->country, "sub-region"));
    } else {
        data->result.value = 0;
    }

    data->i ++;

    return &data->result;
}

void cities_iterRelease(corto_iter *iter) {
    cities_onRequest_data *data = iter->udata;
    json_value_free(data->value);
    if (data->file) {
        corto_fileClose(data->file);
    }
    if (data->json) {
        corto_dealloc(data->json);
    }
    corto_dealloc(data);
}

/* $end */
corto_resultIter _cities_Connector_onRequest(
    cities_Connector this,
    corto_request *request)
{
/* $begin(corto/net/cities/Connector/onRequest) */
    corto_resultIter result;
    corto_file file = NULL;
    corto_string countries = NULL;

    /* If parent is same as mount point, replicator is resolving countries */
    corto_bool findCity = strcmp(request->parent, ".");

    if (findCity) {
        corto_string str = corto_envparse("%s/city_list.json", CORTO_NET_CITIES_ETC);
        file = corto_fileRead(str);
        if (!file) {
            corto_error("failed to load file '%s'", str);
        }
        corto_dealloc(str);
    } else {
        corto_string str = corto_envparse("%s/country_list.json", CORTO_NET_CITIES_ETC);
        countries = corto_fileLoad(str);
        if (!countries) {
            corto_error("failed to load file '%s'", str);
        }
        corto_dealloc(str);
    }

    if (!file && !countries) {
        memset(&result, 0, sizeof(corto_resultIter));
    } else {
        cities_onRequest_data *data = corto_alloc(sizeof(cities_onRequest_data));
        data->file = file;
        data->setContent = request->content;
        data->parent = NULL;

        if (findCity) {
            result.hasNext = cities_iterHasNext;
            result.next = cities_iterNext;
            data->json = NULL;
            data->parent = request->parent;
        } else {
            result.hasNext = cities_iterCountriesHasNext;
            result.next = cities_iterCountriesNext;
            data->json = countries;
            data->value = json_parse_string(countries);
            data->i = 0;
        }
        result.release = cities_iterRelease;
        result.udata = data;
    }

    return result;
/* $end */
}
