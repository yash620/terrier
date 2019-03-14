#include "storage/sql_table.h"
#include <set>
#include <vector>
#include "common/macros.h"

namespace terrier::storage {

SqlTable::SqlTable(BlockStore *const store, const catalog::Schema &schema, const catalog::table_oid_t oid)
    : block_store_(store), oid_(oid) {
  TERRIER_ASSERT(tables_.find(schema.GetVersion()) == tables_.end(), "schema versions for an SQL table must be unique");
  const auto layout_and_map = StorageUtil::BlockLayoutFromSchema(schema);
  tables_[schema.GetVersion()] = {new DataTable(block_store_, layout_and_map.first, layout_version_t(schema.GetVersion())),
                                  layout_and_map.first, layout_and_map.second};
}

SqlTable::~SqlTable() {
  while (tables_.cbegin() != tables_.cend()) {
    auto pair = *(tables_.cbegin());
    delete (pair.second.data_table);  // Delete the data_table object on the heap
    tables_.erase(pair.first);
  }
}

std::vector<col_id_t> SqlTable::ColIdsForOids(const std::vector<catalog::col_oid_t> &col_oids, uint32_t schema_version = 0) const {
  TERRIER_ASSERT(!col_oids.empty(), "Should be used to access at least one column.");
  std::vector<col_id_t> col_ids;

  // Build the input to the initializer constructor
  for (const catalog::col_oid_t col_oid : col_oids) {
    TERRIER_ASSERT(tables_.at(schema_version).column_map.count(col_oid) > 0, "Provided col_oid does not exist in the table.");
    const col_id_t col_id = tables_.at(schema_version).column_map.at(col_oid);
    col_ids.push_back(col_id);
  }

  return col_ids;
}

template <class ProjectionInitializerType>
ProjectionMap SqlTable::ProjectionMapForInitializer(const ProjectionInitializerType &initializer, uint32_t schema_version = 0) const {
  ProjectionMap projection_map;
  // for every attribute in the initializer
  for (uint16_t i = 0; i < initializer.NumColumns(); i++) {
    // extract the underlying col_id it refers to
    const col_id_t col_id_at_offset = initializer.ColId(i);
    // find the key (col_oid) in the table's map corresponding to the value (col_id)
    const auto oid_to_id =
        std::find_if(tables_.at(schema_version).column_map.cbegin(), tables_.at(schema_version).column_map.cend(),
                     [&](const auto &oid_to_id) -> bool { return oid_to_id.second == col_id_at_offset; });
    // insert the mapping from col_oid to projection offset
    projection_map[oid_to_id->first] = i;
  }

  return projection_map;
}

template ProjectionMap SqlTable::ProjectionMapForInitializer<ProjectedColumnsInitializer>(
    const ProjectedColumnsInitializer &initializer) const;
template ProjectionMap SqlTable::ProjectionMapForInitializer<ProjectedRowInitializer>(
    const ProjectedRowInitializer &initializer) const;

}  // namespace terrier::storage
