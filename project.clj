(defproject org.clj-commons/dirigiste "1.0.2"
  :deploy-repositories [["clojars" {:url "https://repo.clojars.org"
                                    :username :env/clojars_username
                                    :password :env/clojars_password
                                    :sign-releases true}]]
  :url "https://github.com/clj-commons/dirigiste"
  :description "Centrally-planned thread and object pools"
  :license {:name "MIT License"}
  :dependencies []
  :profiles {:dev {:dependencies [[org.clojure/clojure "1.11.1"]]}}
  :java-source-paths ["src"]
  :javac-options ["-target" "1.8" "-source" "1.8"]

  ;; Maven properties for the Maven God
  :scm {:url "git@github.com:clj-commons/dirigiste.git"}
  :pom-addition ([:organization
                  [:name "CLJ Commons"]
                  [:url "http://clj-commons.org/"]]
                 [:developers [:developer
                               [:id "kingmob"]
                               [:name "Matthew Davidson"]
                               [:url "http://modulolotus.net"]
                               [:email "matthew@modulolotus.net"]]])
  :classifiers {:javadoc {:java-source-paths ^:replace []
                          :source-paths ^:replace []
                          :resource-paths ^:replace []}
                :sources {:java-source-paths ^:replace ["src"]
                          :resource-paths ^:replace []}})
