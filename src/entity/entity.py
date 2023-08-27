import pandas as pd
import json

class Generic:
    def __init__(self,record: dict) -> None:
        for k,v in record.items():
            setattr(self,k,v)

    @classmethod
    def dict_to_obj(cls,data:dict,ctx):
        return Generic(record=data)
    
    def to_dict(self):
        return self.__dict__
    
    @classmethod
    def get_object(cls,file_path):
        chunk_df=pd.read_csv(file_path,chunksize=10)
        n_row=0
        for df in chunk_df:
            for data in df.values:
                generic=Generic(dict(zip(df.columns,list(map(str,data)))))
                n_row+=1
                yield generic

    @classmethod
    def export_schema_to_confluent_schema(cls,filepath):
        data=next(pd.read_csv(filepath,chunksize=10))
        cols=data.columns
        schema=dict()

        schema.update({
            "type":"record",
            "namespace":"com.mycorp.mynamespace",
            "name":"sampleRecord",
            "doc":"Sample schema to help to understand the schema",
        })
        field=[]
        for col in cols:
            field.append(
                {"name":col,"type":"string","doc":"The string type"}
            )
        schema.update({"fields":field})
        json.dump(schema,open("schema.json","w"))
        return schema   

    @classmethod
    def get_schema_to_produce_consume_data(cls,filepath):
        columns=next(pd.read_csv(filepath,chunksize=10)).columns     
        schema={}
        schema.update(
            {"$id":"http://example.com/myURI.schema.json","$schema":"http://json-schema.org/draft-07/schema#","additionalProperties":False,"description":"Sample schema to help to understand the schema","properties":dict(),"type":"object","title":"SampleRecord"}

        )
        for col in columns:
            schema["properties"].update(
                {col:{"description":f"generic {col}","type":"string"}}
            )
        schema=json.dumps(schema)
        return schema

    def __str__(self):
        return self.__dict__
    

def instance_to_dict(instance:Generic,ctx):
    return instance.to_dict()



