ALTER TABLE cluster ADD labels map<text, text>;
ALTER TABLE scheduler_task DROP tags;
ALTER TABLE scheduler_task ADD labels map<text, text>;