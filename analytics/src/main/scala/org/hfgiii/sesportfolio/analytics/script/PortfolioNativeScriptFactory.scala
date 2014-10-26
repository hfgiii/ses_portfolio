package org.hfgiii.sesportfolio.analytics.script

import org.elasticsearch.common.Nullable
import org.elasticsearch.script.{ExecutableScript, NativeScriptFactory}
import java.util.{Map => JMap}

class PortfolioNativeScriptFactory extends NativeScriptFactory{


  def newScript(@Nullable params: JMap[String, AnyRef]): ExecutableScript =
    new PortfolioScript(params)
}

