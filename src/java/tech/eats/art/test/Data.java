package tech.eats.art.test;


import tech.eats.art.schema.*;


public class Data {
    public static Pedigree makePedigree(int timeSecs) {
        return new Pedigree(timeSecs,
                Source.SELF,
                OrigSystem.page_view(new PageViewSystem())
        );

    }

    public static tech.eats.art.schema.Data makePageview(int userid, String url, int timeSecs) {
        return new tech.eats.art.schema.Data(makePedigree(timeSecs),
                DataUnit.page_view(
                        new PageViewEdge(
                                PersonID.user_id(userid),
                                PageID.url(url),
                                1
                        )));
    }

    public static tech.eats.art.schema.Data makeEquiv(int user1, int user2) {
        return new tech.eats.art.schema.Data(makePedigree(1000),
                DataUnit.equiv(
                        new EquivEdge(
                                PersonID.user_id(user1),
                                PersonID.user_id(user2)
                        )));
    }


}
