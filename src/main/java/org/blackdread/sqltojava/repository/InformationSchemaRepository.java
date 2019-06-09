package org.blackdread.sqltojava.repository;

import org.blackdread.sqltojava.jooq.DefaultSchema;
import org.blackdread.sqltojava.pojo.ColumnInformation;
import org.blackdread.sqltojava.pojo.TableInformation;
import org.blackdread.sqltojava.pojo.TableRelationInformation;
import org.jooq.*;
import org.jooq.impl.TableImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.blackdread.sqltojava.jooq.DefaultSchema.DEFAULT_SCHEMA;


/**
 * <p>Created on 2018/2/8.</p>
 *
 * @author Yoann CAPLAIN
 */
@Repository
public class InformationSchemaRepository {

    private static final Logger log = LoggerFactory.getLogger(InformationSchemaRepository.class);


    public List<TableRelationInformation> getAllTableRelationInformation(final String dbName) {


        List<TableRelationInformation> tableRelationInformations = new ArrayList();
        for (Table table : DEFAULT_SCHEMA.getTables()) {
            for (ForeignKey fk : (List<ForeignKey>) table.getReferences()) {
                TableField tf = (TableField) fk.getKey().getFields().get(0);
                if (fk.getKey().getFields().size() > 1) {
                    System.err.println("error get >1");
                }
                TableRelationInformation tableRelationInformation = new TableRelationInformation(
                    table.getName(), tf.getName(), fk.getKey().getTable().getName(), fk.getFieldsArray()[0].getName()
                );
                tableRelationInformations.add(tableRelationInformation);
            }
        }

        return tableRelationInformations;

        /*
        SELECT CONCAT(table_name) AS table_name, CONCAT(column_name) AS column_name, CONCAT(referenced_table_name)
        AS referenced_table_name, CONCAT(referenced_column_name) AS referenced_column_name
        FROM INFORMATION_SCHEMA.key_column_usage WHERE referenced_table_schema = '" . DB_NAME . "'
        AND referenced_table_name IS NOT NULL ORDER BY table_name, column_name
        */
        /*return create.select(
            KEY_COLUMN_USAGE.TABLE_NAME,
            KEY_COLUMN_USAGE.COLUMN_NAME,
            KEY_COLUMN_USAGE.REFERENCED_TABLE_NAME,
            KEY_COLUMN_USAGE.REFERENCED_COLUMN_NAME)
            .from(InformationSchema.INFORMATION_SCHEMA.KEY_COLUMN_USAGE)
            .where(KEY_COLUMN_USAGE.REFERENCED_TABLE_SCHEMA.eq(dbName)
                .and(KEY_COLUMN_USAGE.REFERENCED_TABLE_NAME.isNotNull()))
            .orderBy(KEY_COLUMN_USAGE.TABLE_NAME, KEY_COLUMN_USAGE.COLUMN_NAME)
            .fetch()
            .map(this::map);
            */
    }

    public List<ColumnInformation> getFullColumnInformationOfTable(final String dbName, final String tableName) {

        List<ColumnInformation> columnInformations = new ArrayList<>();

        Optional<Table<?>> table = DEFAULT_SCHEMA.getTables().stream().filter(t -> t.getName().equals(tableName)).findFirst();
        for (Field field : ((TableImpl) table.get()).fields()) {
            Boolean primarykey = ((Table) table.get()).getPrimaryKey().getFields().stream().anyMatch(t -> field.getName().equals(((TableField) t).getName()));
            ColumnInformation columnInformation = new ColumnInformation(
                field.getName(),
                field.getDataType().getTypeName(),
                null,
                field.getDataType().nullable(),
                primarykey,
                false,
                null,
                field.getName(),
                field.getComment()
            );
            columnInformations.add(columnInformation);


        }


        return columnInformations;
        /*
        return create.resultQuery("SHOW FULL COLUMNS FROM " + dbName + "." + tableName)
//            .bind(1, tableName)
            .fetch()
            .map(r -> new ColumnInformation(
                (String) r.get("Field"),
                (String) r.get("Type"),
                (String) r.get("Collation"),
                (String) r.get("Null"),
                (String) r.get("Key"),
                (String) r.get("Default"),
                (String) r.get("Extra"),
                (String) r.get("Comment")));
    */
    }

    public List<TableInformation> getAllTableInformation(final String dbName) {


        return DefaultSchema.DEFAULT_SCHEMA.getTables().stream().map(r -> new TableInformation(r.getName(), r.getComment())).collect(Collectors.toList())
            ;
        /*

        return create.select(
            InformationSchema.INFORMATION_SCHEMA.TABLES.TABLE_NAME,
            InformationSchema.INFORMATION_SCHEMA.TABLES.TABLE_COMMENT)
            .from(InformationSchema.INFORMATION_SCHEMA.TABLES)
            .where(InformationSchema.INFORMATION_SCHEMA.TABLES.TABLE_SCHEMA.eq(dbName))
            .fetch()
            .map(r -> new TableInformation(r.value1(), r.value2()));
  */

    }

    public List<String> getAllTableName(final String dbName) {


        return DefaultSchema.DEFAULT_SCHEMA.getTables().stream().map(r -> r.getName()).collect(Collectors.toList())
            ;
        /*
        return create.select(
            InformationSchema.INFORMATION_SCHEMA.TABLES.TABLE_NAME)
            .from(InformationSchema.INFORMATION_SCHEMA.TABLES)
            .where(InformationSchema.INFORMATION_SCHEMA.TABLES.TABLE_SCHEMA.eq(dbName))
            .fetch()
            .getValues(InformationSchema.INFORMATION_SCHEMA.TABLES.TABLE_NAME);

  */
    }

    private TableRelationInformation map(final Record4<String, String, String, String> r) {
        return new TableRelationInformation(r.value1(), r.value2(), r.value3(), r.value4());
    }
}
