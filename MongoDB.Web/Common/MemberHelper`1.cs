using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Reflection;
using System.Linq.Expressions;

namespace MongoDB.Web.Common
{
	public class MemberHelper<T>
	{
		/// <summary>
		/// A less-than-ideal way to get a <see cref="System.Reflection.MemberInfo"/> of
		/// a generic type T (instead of the property of an interface which T implements).
		/// </summary>
		/// <remarks>It sure would be nice if .NET would add a memberof(), but in the meantime,
		/// this allows us to get a MemberInfo for a generic type.</remarks>
		/// 
		/// <typeparam name="TReturn">The type of the property</typeparam>
		/// <param name="expression">an expression in the form T => T.Member</param>
		/// <returns></returns>
		public MemberInfo GetMember<TReturn>(Expression<Func<T, TReturn>> expression)
		{
			MemberExpression memberExpression = (MemberExpression)expression.Body;
			return typeof(T).GetProperty(memberExpression.Member.Name);
		}
	}
}
