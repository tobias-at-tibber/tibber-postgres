DROP TABLE IF EXISTS test;
DROP TABLE If EXISTS timestamps;

CREATE TABLE test(
  "integerCol" INTEGER,
  "stringCol" VARCHAR(100),
  "booleanCOl" BOOLEAN
);

INSERT INTO test VALUES (1,'string',true);
INSERT INTO test VALUES (1,null,true);
INSERT INTO test VALUES (2,'test',false);



CREATE TABLE timestamps(
  "id" INTEGER not null,
  CONSTRAINT subscription_element_pk PRIMARY KEY("id"),
  "validFrom" TIMESTAMPTZ,
	"validTo" TIMESTAMPTZ
);

INSERT into timestamps ("id","validFrom", "validTo") VALUES(1, now(), now());
INSERT into timestamps ("id","validFrom", "validTo") VALUES(2, now(), now());