Unique constraints for Google Appengine NDB
===========================================

NDBUnq emulates unique constraints on Google Appengine Datastore using
NDB hooks.

Example
=======

There is full working example for Flask:
https://github.com/vmihailenco/ndbunq-example/:

    import ndbunq
    from google.appengine.ext import ndb


    class User(ndbunq.Model):
        username = ndb.StringProperty(required=True)

        class Meta:
            # username is guaranteed to be unique
            unique = (('username',),)
