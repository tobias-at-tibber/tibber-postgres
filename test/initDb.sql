DROP TABLE IF EXISTS test;
CREATE TABLE test(
  "integerCol" INTEGER,
  "stringCol" VARCHAR(100),
  "booleanCOl" BOOLEAN
);

INSERT INTO test VALUES (1,'string',true);
INSERT INTO test VALUES (1,null,true);
INSERT INTO test VALUES (2,'test',false);