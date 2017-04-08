package net.thornydev;

import java.io.BufferedReader;
import java.io.FileReader;
import java.math.BigInteger;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import javafx.beans.binding.BooleanBinding;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Generates Hive schemas for use with the JSON SerDe from
 * org.openx.data.jsonserde.JsonSerDe.  GitHub link: https://github.com/rcongiu/Hive-JSON-Serde
 * 
 * Pass in a valid JSON document string to {@link JsonHiveSchema#createHiveSchema} and it will
 * return a Hive schema for the JSON document.
 * 
 * It supports embedded JSON objects, arrays and the standard JSON scalar types: strings,
 * numbers, booleans and null.  You probably don't want null in the JSON document you provide
 * as Hive can't use that.  For numbers - if the example value has a decimal, it will be 
 * typed as "double".  If the number has no decimal, it will be typed as "int".
 * 
 * This program uses the JSON parsing code from json.org and that code is included in this
 * library, since it has not been packaged and made available for maven/ivy/gradle dependency
 * resolution.
 * 
 * <strong>Use of main method:</strong> <br>
 *   JsonHiveSchema has a main method that takes a file path to a JSON doc - this file should have
 *   only one JSON file in it.  An optional second argument can be provided to name the Hive table
 *   that is generated.
 */
public class JsonHiveSchema  {
  
  static void help() {
    System.out.println("Usage: Two arguments possible. First is required. Second is optional");
    System.out.println("  1st arg: path to JSON file to parse into Hive schema");
    System.out.println("  2nd arg (optional): tablename.  Defaults to 'x'");
  }
  
  public static void main( String[] args ) throws Exception {
    if (args.length == 0) {
      throw new IllegalArgumentException("ERROR: No file specified");
    }
    
    if (args[0].equals("-h")) {
      help();
      System.exit(0);
    }
    
    StringBuilder sb = new StringBuilder();
    BufferedReader br = new BufferedReader( new FileReader(args[0]) );
    String line;
    while ( (line = br.readLine()) != null ) {
      sb.append(line).append("\n");
    }
    br.close();
    
    String tableName = "x";
    if (args.length == 2) {
      tableName = args[1];
    }

    JsonHiveSchema schemaWriter = new JsonHiveSchema(tableName);
    System.out.println(schemaWriter.createHiveSchema(sb.toString()));
  }
  
  
  private String tableName = "x";
  
  
  public JsonHiveSchema() {}
  
  public JsonHiveSchema(String tableName) {
    this.tableName = tableName;
  }
  
  /**
   * Pass in any valid JSON object and a Hive schema will be returned for it.
   * You should avoid having null values in the JSON document, however.
   * 
   * The Hive schema columns will be printed in alphabetical order - overall and
   * within subsections.
   * 
   * @param json
   * @return string Hive schema
   * @throws JSONException if the JSON does not parse correctly
   */
  public String createHiveSchema(String json) throws JSONException {
    JSONObject jo = new JSONObject(json);
    
    @SuppressWarnings("unchecked")
    Iterator<String> keys = jo.keys();
    keys = new OrderedIterator(keys);
    StringBuilder sb = new StringBuilder("CREATE TABLE ").append(tableName).append(" (\n");

    while (keys.hasNext()) {
      String k = keys.next();

      try {
        String hiveColType = valueToHiveSchema(k, jo.opt(k));
        sb.append("  ");
        sb.append(k.toString());
        sb.append(' ');
        sb.append(hiveColType);
        sb.append(',').append("\n");

      } catch (IllegalStateException e){
        System.err.println("Dropping Key: " + k + " because error:" + e);
      }

    }

    sb.replace(sb.length() - 2, sb.length(), ")\n"); // remove last comma
    return sb.append("ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe';").toString();
  }

  private String toHiveSchema(String colName, JSONObject o) throws JSONException {

    if (objectShouldBeAMap(colName, o)){
      return toHiveMap(colName, o);
    } else {
      return toHiveStruct(colName, o);

    }
  }

  private Boolean objectShouldBeAMap(String colName, JSONObject o){
    if (colName.endsWith("_map")){
      return true;
    }

    Boolean allIntLike = true;
    Iterator<String> keys = o.keys();
    keys = new OrderedIterator(keys);

    while (keys.hasNext()) {
      String k = keys.next();

      if (!k.chars().allMatch( Character::isDigit ))
        return false;
    }

    return allIntLike;
  }

  private String toHiveMap(String colName, JSONObject o) throws JSONException {
    Iterator<String> keys = o.keys();
    keys = new OrderedIterator(keys);

    if (keys.hasNext()){

      StringBuilder sb = new StringBuilder("map<");

      String k = keys.next();
      sb.append("string");
      sb.append(',');
      sb.append(valueToHiveSchema(k, o.opt(k)));
      sb.append(">");

      return sb.toString();

    } else {
      //return "XXXXX";
      throw new IllegalStateException("Empty map");
    }

  }

  private String toHiveStruct(String colName, JSONObject o) throws JSONException {
    Iterator<String> keys = o.keys();
    keys = new OrderedIterator(keys);

    StringBuilder sb = new StringBuilder("struct<");
    
    while (keys.hasNext()) {
      String k = keys.next();

      try {
        String colHiveType = valueToHiveSchema(k, o.opt(k));

        sb.append(k.toString());
        sb.append(':');
        sb.append(colHiveType);
        sb.append(", ");
      } catch (IllegalStateException e){
        System.err.println("Dropping Key: " + k + " because error:" + e);
      }
    }
    sb.replace(sb.length() - 2, sb.length(), ">"); // remove last comma
    return sb.toString();
  }

  private String toHiveSchema(String colName, JSONArray a) throws JSONException {
    return "array<" + arrayJoin(colName, a, ",") + '>';
  }

  private String arrayJoin(String colName, JSONArray a, String separator) throws JSONException {
    StringBuilder sb = new StringBuilder();

    if (a.length() == 0) {
      throw new IllegalStateException("Array is empty: " + a.toString());
    }
    
    Object entry0 = a.get(0);
    if ( isScalar(entry0) ) {
      sb.append( scalarType(entry0) );
    } else if (entry0 instanceof JSONObject) {
      sb.append( toHiveSchema(colName, (JSONObject)entry0) );
    } else if (entry0 instanceof JSONArray) {
      sb.append( toHiveSchema(colName, (JSONArray)entry0) );
    }
    return sb.toString();
  }
  
  private String scalarType(Object o) {
    if (o instanceof String) return "string";
    if (o instanceof Number) return scalarNumericType(o);
    if (o instanceof Boolean) return "boolean";
    return null;
  }

  private String scalarNumericType(Object o) {
    String s = o.toString();
    if (s.indexOf('.') > 0) {
      return "double";
    } else {

      try {
        Integer i = Integer.valueOf(s);
        return "int";
      } catch (NumberFormatException e) {
        return "bigint";
      }
    }
  }

  private boolean isScalar(Object o) {
    return o instanceof String ||
        o instanceof Number ||
        o instanceof Boolean || 
        o == JSONObject.NULL;
  }

  private String valueToHiveSchema(String colName, Object o) throws JSONException {
    if ( isScalar(o) ) {
      return scalarType(o);
    } else if (o instanceof JSONObject) {
      return toHiveSchema(colName, (JSONObject)o);
    } else if (o instanceof JSONArray) {
      return toHiveSchema(colName, (JSONArray) o);
    } else {
      throw new IllegalArgumentException("unknown type: " + o.getClass());
    }
  }
  
  static class OrderedIterator implements Iterator<String> {

    Iterator<String> it;
    
    public OrderedIterator(Iterator<String> iter) {
      SortedSet<String> keys = new TreeSet<String>();
      while (iter.hasNext()) {
        keys.add(iter.next());
      }
      it = keys.iterator();
    }
    
    public boolean hasNext() {
      return it.hasNext();
    }

    public String next() {
      return it.next();
    }

    public void remove() {
      it.remove();
    }
  }
}
