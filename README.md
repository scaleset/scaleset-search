Scaleset Search
==============

Scaleset Search is a Java object model for representing query requests in REST protocols.  It uses Jackson data binding
for JSON serialization.

Quick Start
-----------

### Dependency

```xml
<dependency>
    <groupId>com.scaleset</groupId>
    <artifactId>scaleset-search-api</artifactId>
    <version>0.19.0</version>
</dependency>
```

### Key Concepts

The search model consist of a `Query` object and a `Results` object. 

Query object fields:

| Field     | Description |
|-----------|-------------|
| q         | lucene compatible query string |
| fields    | the list of fields to return |
| offset    | paging offset |
| limit     | paging limit |
| headers   | optional request headers |
| sorts     | result sorting order |
| filters   | query filters|
| aggs      | aggregation to return |

Results object fields:

| Field     | Description |
|-----------|-------------|
| query     |             |
| totalItems|             |
| bbox      |             |
| headers   |             |
| aggs      |             |
| items     |             |

JSON Examples
-----------

### Simple Query and Results

Query

```
{
  "q": "firstName: fred",
  "offset": 0,
  "limit": 10,
  "aggs": [
    {
      "type": "field",
      "name": "lastName",
      "limit": 5,
      "minCount": 1
    }
  ]
}
```

Results

```
{
  "totalItems": 10,
  "items": [
   ]
}
```


### License

Scaleset Search is [Apache 2.0 licensed](http://www.apache.org/licenses/LICENSE-2.0.html).
