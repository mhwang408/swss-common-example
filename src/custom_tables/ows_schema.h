#ifndef OWS_SCHEMA_H
#define OWS_SCHEMA_H

#include "sonic-swss-common/common/schema.h"

#ifdef __cplusplus
namespace swss {
#endif

/*
 * Project-local table names.
 *
 * Keep custom OWS table definitions here instead of modifying the
 * sonic-swss-common submodule.
 */
#define OWS_CFG_CUSTOM_CONFIG_TABLE_NAME "CUSTOM_CONFIG_TABLE"
#define OWS_APP_CUSTOM_APPL_TABLE_NAME   "CUSTOM_APPL_TABLE"

#ifdef __cplusplus
}
#endif

#endif
